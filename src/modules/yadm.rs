use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use gix::ThreadSafeRepository;
use gix::bstr::ByteVec;
use gix::status::Submodule;
use regex::Regex;

use super::git_status::uses_reftables;
use super::{Context, Module, ModuleConfig};

use crate::configs::yadm::YadmConfig;
use crate::formatter::StringFormatter;
use crate::utils::{create_command, exec_timeout};
use crate::{num_configured_starship_threads, num_rayon_threads};

/// Shows when the YADM bare repository has uncommitted changes on tracked files or unpushed commits
/// (local branch ahead of its upstream).
pub fn module<'a>(context: &'a Context) -> Option<Module<'a>> {
    let mut module = context.new_module("yadm");
    let config = YadmConfig::try_load(module.config);

    if config.disabled {
        return None;
    }

    let home = context.get_home()?;
    let repo_path = resolve_repo_path(context, &config)?;

    if !is_yadm_dirty_or_ahead(context, &repo_path, &home) {
        return None;
    }

    let parsed = StringFormatter::new(config.format).and_then(|formatter| {
        formatter
            .map_meta(|var, _| match var {
                "symbol" => Some(config.symbol),
                _ => None,
            })
            .map_style(|variable| match variable {
                "style" => Some(Ok(config.style)),
                _ => None,
            })
            .parse(None, Some(context))
    });

    module.set_segments(match parsed {
        Ok(segments) => segments,
        Err(error) => {
            log::warn!("Error in module `yadm`:\n{error}");
            return None;
        }
    });

    Some(module)
}

/// Resolves the YADM bare repository path according to the following priority order:
/// 1. `[yadm].repo_path` in Starship config
/// 2. `$YADM_REPO`
/// 3. `$XDG_DATA_HOME/yadm/repo.git` (defaulting `XDG_DATA_HOME` to `$HOME/.local/share`), then `$HOME/.yadm/repo.git`
fn resolve_repo_path(context: &Context, config: &YadmConfig<'_>) -> Option<PathBuf> {
    if let Some(p) = config.repo_path.map(str::trim).filter(|s| !s.is_empty()) {
        return Some(Context::expand_tilde(PathBuf::from(p)));
    }
    if let Some(p) = context.get_env("YADM_REPO") {
        let p = p.trim();
        if !p.is_empty() {
            return Some(Context::expand_tilde(PathBuf::from(p)));
        }
    }
    let home = context.get_home()?;
    let data_home = context
        .get_env("XDG_DATA_HOME")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"));
    let xdg_path = data_home.join("yadm/repo.git");
    let legacy_path = home.join(".yadm/repo.git");
    if is_plausible_yadm_git_dir(&xdg_path) {
        return Some(xdg_path);
    }
    if is_plausible_yadm_git_dir(&legacy_path) {
        return Some(legacy_path);
    }
    None
}

fn is_plausible_yadm_git_dir(path: &Path) -> bool {
    path.join("HEAD").is_file() && path.join("config").is_file()
}

/// Match [`Context::get_repo`] permission defaults so YADM reads the same config sources as other Git modules.
fn yadm_repository_open_options(worktree: &Path) -> gix::open::Options {
    use gix::open::{Options, Permissions, permissions::Config as GitOpenConfig};

    let config_perms = GitOpenConfig {
        git_binary: true,
        system: true,
        git: true,
        user: true,
        env: true,
        includes: true,
    };
    let permissions = Permissions {
        config: config_perms,
        ..Permissions::all()
    };
    let worktree_display = worktree.display().to_string();
    Options::default()
        .permissions(permissions)
        .config_overrides([format!("core.worktree={worktree_display}")])
}

fn open_yadm_thread_safe_repository(
    repo_path: &Path,
    worktree: &Path,
) -> Option<ThreadSafeRepository> {
    ThreadSafeRepository::open_opts(repo_path, yadm_repository_open_options(worktree)).ok()
}

/// Parses `# branch.ab +AHEAD -BEHIND` from `git status --porcelain=v2 --branch` (see `git_status::RepoStatus::set_ahead_behind`).
static BRANCH_AB_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"# branch\.ab \+([0-9]+) \-([0-9]+)").expect("valid regex"));

fn parse_porcelain_v2_branch_ab_ahead(line: &str) -> Option<usize> {
    BRANCH_AB_RE
        .captures(line)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

/// Parses `git for-each-ref --format='%(upstream) %(upstream:track)'` (mirrors `git_status::RepoStatus::set_ahead_behind_for_each_ref`).
fn upstream_ahead_from_for_each_ref_line(line: &str) -> usize {
    let line = line.trim();
    if line.is_empty() || line == " " || line.ends_with(" [gone]") {
        return 0;
    }
    let track = match line.split_once(' ') {
        Some((_, rest)) => rest,
        None => return 0,
    };
    let track = track.trim_matches(|c| c == '[' || c == ']');
    let mut ahead = None;
    let mut behind = None;
    for pair in track.split(',') {
        let mut tokens = pair.trim().splitn(2, ' ');
        if let (Some(name), Some(number)) = (tokens.next(), tokens.next()) {
            match name {
                "ahead" => ahead = number.parse().ok(),
                "behind" => behind = number.parse().ok(),
                _ => {}
            }
        }
    }
    for field in [&mut ahead, &mut behind] {
        if field.is_none() {
            *field = Some(0);
        }
    }
    ahead.unwrap_or(0)
}

/// Uses `git for-each-ref` with a subprocess timeout (same approach as the gix path in `git_status::get_repo_status`).
fn upstream_ahead_count_via_git(
    repo_path: &Path,
    worktree: &Path,
    branch_full_name: &str,
    command_timeout_ms: u64,
) -> Option<usize> {
    let mut cmd = create_command("git").ok()?;
    cmd.env("GIT_OPTIONAL_LOCKS", "0")
        .arg("-C")
        .arg(worktree)
        .arg("--git-dir")
        .arg(repo_path)
        .args([
            "for-each-ref",
            "--format=%(upstream) %(upstream:track)",
            branch_full_name,
        ]);
    let out = exec_timeout(&mut cmd, Duration::from_millis(command_timeout_ms))?;
    let line = out.stdout.lines().next().unwrap_or_default();
    Some(upstream_ahead_from_for_each_ref_line(line))
}

/// Fallback aligned with `git_status::get_repo_status` when using the Git executable: porcelain v2 plus `--branch`.
fn is_yadm_dirty_or_ahead_via_git_executable(
    repo_path: &Path,
    worktree: &Path,
    command_timeout_ms: u64,
) -> Option<bool> {
    let mut cmd = create_command("git").ok()?;
    cmd.env("GIT_OPTIONAL_LOCKS", "0")
        .arg("-C")
        .arg(worktree)
        .arg("--git-dir")
        .arg(repo_path)
        .arg("-c")
        .arg("core.fsmonitor=")
        .args([
            "status",
            "--porcelain=2",
            "--branch",
            "--untracked-files=no",
            "--ignore-submodules=dirty",
        ]);
    let out = exec_timeout(&mut cmd, Duration::from_millis(command_timeout_ms))?;
    let mut dirty = false;
    let mut ahead = false;
    for line in out.stdout.lines() {
        if line.starts_with("# branch.ab ") {
            if parse_porcelain_v2_branch_ab_ahead(line).is_some_and(|n| n > 0) {
                ahead = true;
            }
        } else if !line.is_empty() && !line.starts_with('#') {
            dirty = true;
        }
    }
    Some(dirty || ahead)
}

/// Native status via gix; upstream ahead count via `git for-each-ref` (same split as `git_status::get_repo_status`).
fn is_yadm_dirty_or_ahead_via_gix(
    gix_repo: &gix::Repository,
    repo_path: &Path,
    worktree: &Path,
    command_timeout_ms: u64,
) -> Option<bool> {
    let is_interrupted = Arc::new(AtomicBool::new(false));
    std::thread::Builder::new()
        .name("starship yadm timer".into())
        .stack_size(256 * 1024)
        .spawn({
            let is_interrupted = is_interrupted.clone();
            let abort_after = Duration::from_millis(command_timeout_ms);
            move || {
                std::thread::sleep(abort_after);
                is_interrupted.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        })
        .expect("should be able to spawn timer thread");

    let check_dirty = true;
    let status = gix_repo
        .status(gix::features::progress::Discard)
        .ok()?
        .index_worktree_submodules(Submodule::Given {
            ignore: gix::submodule::config::Ignore::Dirty,
            check_dirty,
        })
        .index_worktree_options_mut(|opts| {
            opts.thread_limit = if cfg!(target_os = "macos") {
                Some(num_configured_starship_threads().unwrap_or(3))
            } else {
                Some(num_rayon_threads())
            };
            opts.dirwalk_options.take();
        })
        .tree_index_track_renames(gix::status::tree_index::TrackRenames::Disabled)
        .should_interrupt_owned(is_interrupted.clone());

    let status = status.into_iter(None).ok()?;

    let branch_full_name = gix_repo.head_name().ok().flatten().and_then(|ref_name| {
        Vec::from(gix::bstr::BString::from(ref_name))
            .into_string()
            .ok()
    });
    let upstream_ahead = branch_full_name
        .as_deref()
        .and_then(|name| {
            upstream_ahead_count_via_git(repo_path, worktree, name, command_timeout_ms)
        })
        .unwrap_or(0);

    if is_interrupted.load(std::sync::atomic::Ordering::Relaxed) {
        return None;
    }

    if upstream_ahead > 0 {
        return Some(true);
    }

    for item in status {
        if is_interrupted.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        if item.ok().is_some() {
            return Some(true);
        }
    }

    if is_interrupted.load(std::sync::atomic::Ordering::Relaxed) {
        return None;
    }

    Some(false)
}

fn is_yadm_dirty_or_ahead(context: &Context, repo_path: &Path, worktree: &Path) -> bool {
    let Some(ts_repo) = open_yadm_thread_safe_repository(repo_path, worktree) else {
        log::debug!("yadm: failed to open repository at {}", repo_path.display());
        return false;
    };
    let gix_repo = ts_repo.to_thread_local();
    let timeout_ms = context.root_config.command_timeout;

    if gix_repo
        .index_or_empty()
        .ok()
        .is_some_and(|idx| idx.is_sparse())
        || uses_reftables(&gix_repo)
        || gix_repo
            .config_snapshot()
            .boolean("core.fsmonitor")
            .unwrap_or(false)
    {
        return is_yadm_dirty_or_ahead_via_git_executable(repo_path, worktree, timeout_ms)
            .unwrap_or(false);
    }

    is_yadm_dirty_or_ahead_via_gix(&gix_repo, repo_path, worktree, timeout_ms).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{is_yadm_dirty_or_ahead, resolve_repo_path};
    use crate::config::ModuleConfig;
    use crate::configs::yadm::YadmConfig;
    use crate::context::{Context, Env, Properties, Shell, Target};
    use crate::utils::create_command;
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};

    /// Same identity and fsync defaults as `crate::test::fixture_repo` / `git_metrics` tests.
    fn configure_git_user_and_fsync(repo: &Path) -> io::Result<()> {
        let email = create_command("git")?
            .args(["config", "--local", "user.email", "starship@example.com"])
            .current_dir(repo)
            .output()?;
        if !email.status.success() {
            return Err(io::Error::other(format!(
                "git config user.email failed: {}",
                String::from_utf8_lossy(&email.stderr)
            )));
        }
        let name = create_command("git")?
            .args(["config", "--local", "user.name", "starship"])
            .current_dir(repo)
            .output()?;
        if !name.status.success() {
            return Err(io::Error::other(format!(
                "git config user.name failed: {}",
                String::from_utf8_lossy(&name.stderr)
            )));
        }
        let _ = create_command("git")?
            .args(["config", "--local", "core.fsync", "all"])
            .current_dir(repo)
            .output();
        let _ = create_command("git")?
            .args(["config", "--local", "core.fsyncObjectFiles", "true"])
            .current_dir(repo)
            .output();
        Ok(())
    }

    /// Matches `git_metrics::create_repo_with_commit`: `init --quiet`, Starship test user, then
    /// `checkout -b master` (may fail if already on `master`).
    fn init_git_repo_starship_style(repo: &Path) -> io::Result<()> {
        let init = create_command("git")?
            .args(["init", "--quiet"])
            .current_dir(repo)
            .output()?;
        if !init.status.success() {
            return Err(io::Error::other(format!(
                "git init failed: {}",
                String::from_utf8_lossy(&init.stderr)
            )));
        }
        configure_git_user_and_fsync(repo)?;
        let _ = create_command("git")?
            .args(["checkout", "-b", "master"])
            .current_dir(repo)
            .output();
        Ok(())
    }

    fn context_with_env(env: Env<'static>) -> Context<'static> {
        Context::new_with_shell_and_path(
            Properties::default(),
            Shell::Unknown,
            Target::Main,
            PathBuf::from("/"),
            PathBuf::from("/"),
            env,
        )
    }

    #[test]
    fn resolve_prefers_config_repo_path() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let marker = tmp.path().join("bare.git");
        fs::create_dir_all(marker.join("objects"))?;
        fs::write(marker.join("HEAD"), "ref: refs/heads/master\n")?;
        fs::write(
            marker.join("config"),
            "[core]\n\tbare = true\nrepositoryformatversion = 0\n",
        )?;

        let mut yadm_table = toml::Table::new();
        yadm_table.insert(
            "repo_path".into(),
            toml::Value::String(marker.to_string_lossy().into_owned()),
        );
        let mut root = toml::Table::new();
        root.insert("yadm".into(), toml::Value::Table(yadm_table));

        let mut env = Env::default();
        env.insert("HOME", tmp.path().to_string_lossy().into_owned());
        env.insert("YADM_REPO", "/should/not/use".into());
        let ctx = context_with_env(env).set_config(root);

        let config = YadmConfig::try_load(ctx.config.get_module_config("yadm"));
        let resolved = resolve_repo_path(&ctx, &config).expect("repo path");
        assert_eq!(resolved, marker);

        tmp.close()
    }

    #[test]
    fn resolve_uses_yadm_repo_env_when_no_config_path() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let marker = tmp.path().join("from-env.git");
        fs::create_dir_all(marker.join("objects"))?;
        fs::write(marker.join("HEAD"), "ref: refs/heads/master\n")?;
        fs::write(
            marker.join("config"),
            "[core]\n\tbare = true\nrepositoryformatversion = 0\n",
        )?;

        let mut env = Env::default();
        env.insert("HOME", tmp.path().to_string_lossy().into_owned());
        env.insert("YADM_REPO", marker.to_string_lossy().into_owned());
        let ctx = context_with_env(env);
        let config = YadmConfig::try_load(ctx.config.get_module_config("yadm"));
        let resolved = resolve_repo_path(&ctx, &config).expect("repo path");
        assert_eq!(resolved, marker);

        tmp.close()
    }

    #[test]
    fn is_yadm_dirty_or_ahead_detects_modified_tracked_file() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let home = tmp.path().join("home");
        fs::create_dir_all(&home)?;
        let repo_git = tmp.path().join("repo.git");

        fs::create_dir_all(home.join(".config"))?;
        fs::write(home.join(".config/foo.toml"), "a = 1\n")?;
        init_git_repo_starship_style(&home)?;
        create_command("git")?
            .args(["add", ".config/foo.toml"])
            .current_dir(&home)
            .output()?;
        let commit = create_command("git")?
            .args(["commit", "-m", "init", "--no-gpg-sign"])
            .current_dir(&home)
            .output()?;
        assert!(
            commit.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&commit.stderr)
        );

        let _ = fs::remove_dir_all(&repo_git);
        let home_name = home.file_name().unwrap().to_str().unwrap();
        let repo_name = repo_git.file_name().unwrap().to_str().unwrap();
        let clone_bare = create_command("git")?
            .args(["clone", "--bare", home_name, repo_name])
            .current_dir(tmp.path())
            .output()?;
        assert!(
            clone_bare.status.success(),
            "git clone --bare failed: {}",
            String::from_utf8_lossy(&clone_bare.stderr)
        );

        let mut env = Env::default();
        env.insert("HOME", home.to_string_lossy().into_owned());
        let ctx = context_with_env(env);

        fs::write(home.join(".config/foo.toml"), "a = 2\n")?;
        assert!(
            is_yadm_dirty_or_ahead(&ctx, &repo_git, &home),
            "modified tracked file should be detected"
        );

        tmp.close()
    }

    #[test]
    fn is_yadm_dirty_or_ahead_detects_ahead_of_upstream() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let server_git = tmp.path().join("server.git");
        fs::create_dir_all(server_git.join("objects"))?;
        let bare_init = create_command("git")?
            .args(["init", "--bare", "-b", "master"])
            .arg(&server_git)
            .output()?;
        if !bare_init.status.success() {
            let out = create_command("git")?
                .args(["init", "--bare"])
                .arg(&server_git)
                .output()?;
            assert!(
                out.status.success(),
                "git init --bare failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }

        let home = tmp.path().join("home");
        fs::create_dir_all(&home)?;
        fs::write(home.join("tracked.txt"), "v1\n")?;
        init_git_repo_starship_style(&home)?;
        create_command("git")?
            .args(["add", "tracked.txt"])
            .current_dir(&home)
            .output()?;
        let commit = create_command("git")?
            .args(["commit", "-m", "init", "--no-gpg-sign"])
            .current_dir(&home)
            .output()?;
        assert!(
            commit.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&commit.stderr)
        );
        create_command("git")?
            .args(["remote", "add", "origin", server_git.to_str().unwrap()])
            .current_dir(&home)
            .output()?;
        let push = create_command("git")?
            .args(["push", "-u", "origin", "HEAD:master"])
            .current_dir(&home)
            .output()?;
        assert!(
            push.status.success(),
            "git push failed: {}",
            String::from_utf8_lossy(&push.stderr)
        );

        // Mirror the remote bare repo (not `clone --bare` from `home`): a bare clone of a normal
        // repo drops remote-tracking refs, so `for-each-ref` never sees an upstream until we
        // re-fetch with a refspec. Mirroring `server.git` matches real YADM bare layouts.
        let repo_git = tmp.path().join("yadm.git");
        let _ = fs::remove_dir_all(&repo_git);
        let server_name = server_git.file_name().unwrap().to_str().unwrap();
        let yadm_repo_name = repo_git.file_name().unwrap().to_str().unwrap();
        let mirror = create_command("git")?
            .args(["clone", "--mirror", server_name, yadm_repo_name])
            .current_dir(tmp.path())
            .output()?;
        assert!(
            mirror.status.success(),
            "git clone --mirror failed: {}",
            String::from_utf8_lossy(&mirror.stderr)
        );

        let home_s = home.to_str().unwrap();
        let repo_s = repo_git.to_str().unwrap();

        let fetch_refspec = create_command("git")?
            .args([
                "--git-dir",
                repo_s,
                "config",
                "remote.origin.fetch",
                "+refs/heads/*:refs/remotes/origin/*",
            ])
            .output()?;
        assert!(
            fetch_refspec.status.success(),
            "git config fetch refspec failed: {}",
            String::from_utf8_lossy(&fetch_refspec.stderr)
        );

        let fetch = create_command("git")?
            .args(["--git-dir", repo_s, "fetch", "origin"])
            .output()?;
        assert!(
            fetch.status.success(),
            "git fetch failed: {}",
            String::from_utf8_lossy(&fetch.stderr)
        );

        for (key, val) in [
            ("branch.master.remote", "origin"),
            ("branch.master.merge", "refs/heads/master"),
        ] {
            let cfg = create_command("git")?
                .args(["--git-dir", repo_s, "config", key, val])
                .output()?;
            assert!(
                cfg.status.success(),
                "git config {key} failed: {}",
                String::from_utf8_lossy(&cfg.stderr)
            );
        }

        for (key, val) in [
            ("user.email", "starship@example.com"),
            ("user.name", "starship"),
        ] {
            let id = create_command("git")?
                .args(["--git-dir", repo_s, "config", key, val])
                .output()?;
            assert!(
                id.status.success(),
                "git config {key} on bare repo failed: {}",
                String::from_utf8_lossy(&id.stderr)
            );
        }

        // Drop the disposable worktree repo so only the YADM-style bare + `$HOME` layout remains.
        fs::remove_dir_all(home.join(".git"))?;
        let checkout = create_command("git")?
            .args([
                "--work-tree",
                home_s,
                "--git-dir",
                repo_s,
                "checkout",
                "-f",
                "master",
            ])
            .output()?;
        assert!(
            checkout.status.success(),
            "git checkout failed: {}",
            String::from_utf8_lossy(&checkout.stderr)
        );

        let ahead_commit = create_command("git")?
            .args([
                "--work-tree",
                home_s,
                "--git-dir",
                repo_s,
                "commit",
                "--allow-empty",
                "-m",
                "ahead",
                "--no-gpg-sign",
            ])
            .output()?;
        assert!(
            ahead_commit.status.success(),
            "git commit (ahead) failed: {}",
            String::from_utf8_lossy(&ahead_commit.stderr)
        );

        let mut env = Env::default();
        env.insert("HOME", home.to_string_lossy().into_owned());
        let ctx = context_with_env(env);

        assert!(
            is_yadm_dirty_or_ahead(&ctx, &repo_git, &home),
            "local branch ahead of upstream should be detected"
        );

        tmp.close()
    }
}
