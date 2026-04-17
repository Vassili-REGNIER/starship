use std::path::{Path, PathBuf};
use std::time::Duration;

use super::{Context, Module, ModuleConfig};

use crate::configs::yadm::YadmConfig;
use crate::formatter::StringFormatter;
use crate::utils::{create_command, exec_timeout};

/// Shows when the YADM bare repository has uncommitted changes on tracked files
/// or unpushed commits (local branch ahead of its upstream).
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

/// Resolves the YADM bare repository path with the following priority:
/// 1. `[yadm].repo_path` from Starship config
/// 2. `$YADM_REPO` environment variable
/// 3. `$XDG_DATA_HOME/yadm/repo.git` (falling back to `$HOME/.local/share`),
///    then `$HOME/.yadm/repo.git`
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

/// Checks whether the YADM repository has any local changes on tracked files
/// or unpushed commits by shelling out to `git status --porcelain=2 --branch`.
///
/// YADM typically uses a bare repository with `$HOME` as the work tree, so the
/// `--work-tree` and `--git-dir` flags are provided explicitly.
fn is_yadm_dirty_or_ahead(context: &Context, repo_path: &Path, worktree: &Path) -> bool {
    let timeout = Duration::from_millis(context.root_config.command_timeout);

    let Ok(mut cmd) = create_command("git") else {
        return false;
    };
    cmd.env("GIT_OPTIONAL_LOCKS", "0")
        .arg("--work-tree")
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

    let Some(output) = exec_timeout(&mut cmd, timeout) else {
        log::debug!(
            "yadm: `git status` timed out or failed for {}",
            repo_path.display()
        );
        return false;
    };

    for line in output.stdout.lines() {
        if let Some(rest) = line.strip_prefix("# branch.ab ") {
            if parse_ahead_count(rest).is_some_and(|n| n > 0) {
                return true;
            }
        } else if !line.is_empty() && !line.starts_with('#') {
            return true;
        }
    }
    false
}

/// Extracts the ahead counter from the `+AHEAD -BEHIND` tail of a porcelain v2
/// `# branch.ab` line.
fn parse_ahead_count(rest: &str) -> Option<usize> {
    rest.split_whitespace()
        .next()?
        .strip_prefix('+')?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::{is_yadm_dirty_or_ahead, module, resolve_repo_path};
    use crate::config::ModuleConfig;
    use crate::configs::yadm::YadmConfig;
    use crate::context::{Context, Env, Properties, Shell, Target};
    use crate::utils::create_command;
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};

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

    fn write_plausible_bare_git_layout(git_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(git_dir.join("objects"))?;
        fs::write(git_dir.join("HEAD"), "ref: refs/heads/master\n")?;
        fs::write(
            git_dir.join("config"),
            "[core]\n\tbare = true\nrepositoryformatversion = 0\n",
        )?;
        Ok(())
    }

    #[test]
    fn module_returns_none_when_disabled() -> io::Result<()> {
        let mut yadm_table = toml::Table::new();
        yadm_table.insert("disabled".into(), toml::Value::Boolean(true));
        let mut root = toml::Table::new();
        root.insert("yadm".into(), toml::Value::Table(yadm_table));

        let ctx = context_with_env(Env::default()).set_config(root);
        assert!(
            module(&ctx).is_none(),
            "module should be None when disabled"
        );
        Ok(())
    }

    #[test]
    fn resolve_returns_none_when_no_default_yadm_repo() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut env = Env::default();
        env.insert("HOME", tmp.path().to_string_lossy().into_owned());
        let ctx = context_with_env(env);
        let config = YadmConfig::try_load(ctx.config.get_module_config("yadm"));
        assert!(resolve_repo_path(&ctx, &config).is_none());
        tmp.close()
    }

    #[test]
    fn resolve_prefers_xdg_default_path_over_legacy_dot_yadm() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let home = tmp.path().join("home");
        fs::create_dir_all(&home)?;
        let xdg = home.join(".local/share/yadm/repo.git");
        let legacy = home.join(".yadm/repo.git");
        write_plausible_bare_git_layout(&xdg)?;
        write_plausible_bare_git_layout(&legacy)?;

        let mut env = Env::default();
        env.insert("HOME", home.to_string_lossy().into_owned());
        let ctx = context_with_env(env);
        let config = YadmConfig::try_load(ctx.config.get_module_config("yadm"));
        let resolved = resolve_repo_path(&ctx, &config).expect("repo path");
        assert_eq!(
            resolved, xdg,
            "XDG-style default path should win over ~/.yadm/repo.git"
        );
        tmp.close()
    }

    #[test]
    fn resolve_uses_xdg_data_home_for_default_path() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let home = tmp.path().join("home");
        fs::create_dir_all(&home)?;
        let custom = tmp.path().join("custom-data");
        let repo = custom.join("yadm/repo.git");
        write_plausible_bare_git_layout(&repo)?;

        let mut env = Env::default();
        env.insert("HOME", home.to_string_lossy().into_owned());
        env.insert("XDG_DATA_HOME", custom.to_string_lossy().into_owned());
        let ctx = context_with_env(env);
        let config = YadmConfig::try_load(ctx.config.get_module_config("yadm"));
        let resolved = resolve_repo_path(&ctx, &config).expect("repo path");
        assert_eq!(resolved, repo);
        tmp.close()
    }

    #[test]
    fn resolve_prefers_config_repo_path() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let marker = tmp.path().join("bare.git");
        write_plausible_bare_git_layout(&marker)?;

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
        write_plausible_bare_git_layout(&marker)?;

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
    fn parse_ahead_count_extracts_ahead_only() {
        assert_eq!(super::parse_ahead_count("+5 -0"), Some(5));
        assert_eq!(super::parse_ahead_count("+0 -3"), Some(0));
        assert_eq!(super::parse_ahead_count("master"), None);
    }

    #[test]
    fn module_returns_none_when_repo_is_clean() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let home = tmp.path().join("home");
        fs::create_dir_all(&home)?;
        let repo_git = tmp.path().join("repo.git");

        fs::create_dir_all(home.join(".config"))?;
        fs::write(home.join(".config/foo.toml"), "clean\n")?;
        init_git_repo_starship_style(&home)?;
        create_command("git")?
            .args(["add", ".config/foo.toml"])
            .current_dir(&home)
            .output()?;
        let commit = create_command("git")?
            .args(["commit", "-m", "init", "--no-gpg-sign"])
            .current_dir(&home)
            .output()?;
        assert!(commit.status.success());

        let _ = fs::remove_dir_all(&repo_git);
        let home_name = home.file_name().unwrap().to_str().unwrap();
        let repo_name = repo_git.file_name().unwrap().to_str().unwrap();
        let clone_bare = create_command("git")?
            .args(["clone", "--bare", home_name, repo_name])
            .current_dir(tmp.path())
            .output()?;
        assert!(clone_bare.status.success());

        fs::remove_dir_all(home.join(".git"))?;
        let checkout = create_command("git")?
            .args([
                "--work-tree",
                home.to_str().unwrap(),
                "--git-dir",
                repo_git.to_str().unwrap(),
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

        let mut yadm_table = toml::Table::new();
        yadm_table.insert(
            "repo_path".into(),
            toml::Value::String(repo_git.to_string_lossy().into_owned()),
        );
        let mut root = toml::Table::new();
        root.insert("yadm".into(), toml::Value::Table(yadm_table));

        let mut env = Env::default();
        env.insert("HOME", home.to_string_lossy().into_owned());
        let ctx = context_with_env(env).set_config(root);

        assert!(
            module(&ctx).is_none(),
            "module should be hidden when there are no local changes and no ahead commits"
        );
        tmp.close()
    }

    #[test]
    fn module_returns_some_when_repo_is_dirty() -> io::Result<()> {
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
        assert!(commit.status.success());

        let _ = fs::remove_dir_all(&repo_git);
        let home_name = home.file_name().unwrap().to_str().unwrap();
        let repo_name = repo_git.file_name().unwrap().to_str().unwrap();
        let clone_bare = create_command("git")?
            .args(["clone", "--bare", home_name, repo_name])
            .current_dir(tmp.path())
            .output()?;
        assert!(clone_bare.status.success());

        fs::remove_dir_all(home.join(".git"))?;
        assert!(
            create_command("git")?
                .args([
                    "--work-tree",
                    home.to_str().unwrap(),
                    "--git-dir",
                    repo_git.to_str().unwrap(),
                    "checkout",
                    "-f",
                    "master",
                ])
                .output()?
                .status
                .success()
        );

        let mut yadm_table = toml::Table::new();
        yadm_table.insert(
            "repo_path".into(),
            toml::Value::String(repo_git.to_string_lossy().into_owned()),
        );
        let mut root = toml::Table::new();
        root.insert("yadm".into(), toml::Value::Table(yadm_table));

        let mut env = Env::default();
        env.insert("HOME", home.to_string_lossy().into_owned());
        let ctx = context_with_env(env).set_config(root);

        fs::write(home.join(".config/foo.toml"), "a = 2\n")?;
        assert!(
            module(&ctx).is_some(),
            "module should render when the YADM repo has working tree changes"
        );
        tmp.close()
    }

    #[test]
    fn module_returns_none_on_invalid_format_string() -> io::Result<()> {
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
        assert!(
            create_command("git")?
                .args(["commit", "-m", "init", "--no-gpg-sign"])
                .current_dir(&home)
                .output()?
                .status
                .success()
        );

        let _ = fs::remove_dir_all(&repo_git);
        let clone_bare = create_command("git")?
            .args([
                "clone",
                "--bare",
                home.file_name().unwrap().to_str().unwrap(),
                repo_git.file_name().unwrap().to_str().unwrap(),
            ])
            .current_dir(tmp.path())
            .output()?;
        assert!(clone_bare.status.success());

        fs::remove_dir_all(home.join(".git"))?;
        assert!(
            create_command("git")?
                .args([
                    "--work-tree",
                    home.to_str().unwrap(),
                    "--git-dir",
                    repo_git.to_str().unwrap(),
                    "checkout",
                    "-f",
                    "master",
                ])
                .output()?
                .status
                .success()
        );

        let mut yadm_table = toml::Table::new();
        yadm_table.insert(
            "repo_path".into(),
            toml::Value::String(repo_git.to_string_lossy().into_owned()),
        );
        yadm_table.insert(
            "format".into(),
            toml::Value::String("[$symbol]($style".into()),
        );
        let mut root = toml::Table::new();
        root.insert("yadm".into(), toml::Value::Table(yadm_table));

        let mut env = Env::default();
        env.insert("HOME", home.to_string_lossy().into_owned());
        let ctx = context_with_env(env).set_config(root);

        fs::write(home.join(".config/foo.toml"), "a = 2\n")?;
        assert!(
            module(&ctx).is_none(),
            "invalid format should yield None after formatter error"
        );
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

        // Mirror the remote bare repo to match a real YADM layout: a plain
        // `clone --bare` from a normal repo drops remote-tracking refs so the
        // upstream tracking info is never recorded.
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
