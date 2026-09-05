#![deny(unsafe_code)]

use std::process::ExitCode;

use woven_server::{ServerConfig, serve};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivityLogMode {
    None,
    All,
    Transform,
}

impl ActivityLogMode {
    fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut mode = Self::None;
        let mut selected = false;
        for argument in arguments {
            let parsed = match argument.as_str() {
                "--log-none" => Self::None,
                "--log-all" => Self::All,
                "--log-transform" => Self::Transform,
                "--help" | "-h" => return Err(Self::usage().to_owned()),
                _ => return Err(format!("unknown argument: {argument}\n\n{}", Self::usage())),
            };
            if selected {
                return Err(format!(
                    "choose exactly one activity log mode\n\n{}",
                    Self::usage()
                ));
            }
            mode = parsed;
            selected = true;
        }
        Ok(mode)
    }

    const fn usage() -> &'static str {
        "Usage: woven-server [--log-none | --log-all | --log-transform]\n\
         \n\
         Development activity logging is disabled by default.\n\
         --log-all        Print all safe activity metadata to stdout in debug builds.\n\
         --log-transform  Print entity position and entity-scoped latest-state activity only.\n\
         --log-none       Disable development activity logging (the default)."
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let activity_log_mode = match ActivityLogMode::parse(std::env::args().skip(1)) {
        Ok(mode) => mode,
        Err(message) => {
            println!("{message}");
            return ExitCode::FAILURE;
        }
    };
    if !activity_logging_supported(activity_log_mode) {
        eprintln!("Activity logging is available only in debug builds; release binaries omit it.");
        return ExitCode::FAILURE;
    }

    init_logging(activity_log_mode);
    match serve(ServerConfig::default()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Woven node stopped: {error}");
            ExitCode::FAILURE
        }
    }
}

const fn activity_logging_supported(mode: ActivityLogMode) -> bool {
    #[cfg(debug_assertions)]
    {
        let _ = mode;
        true
    }
    #[cfg(not(debug_assertions))]
    {
        mode == ActivityLogMode::None
    }
}

fn init_logging(activity_log_mode: ActivityLogMode) {
    #[cfg(debug_assertions)]
    {
        let filter = match activity_log_mode {
            ActivityLogMode::None => "warn,woven_activity=off",
            ActivityLogMode::All => "warn,woven_activity=info",
            ActivityLogMode::Transform => "warn,woven_activity=off,woven_activity::transform=info",
        };
        tracing_subscriber::fmt()
            .compact()
            .with_target(false)
            .with_writer(std::io::stdout)
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .init();
    }

    #[cfg(not(debug_assertions))]
    {
        let _ = activity_log_mode;
        tracing_subscriber::fmt::init();
    }
}

#[cfg(test)]
mod tests {
    use super::ActivityLogMode;

    #[test]
    fn activity_log_modes_are_explicit_and_exclusive() {
        assert_eq!(
            ActivityLogMode::parse(["--log-all".to_owned()].into_iter()),
            Ok(ActivityLogMode::All)
        );
        assert_eq!(
            ActivityLogMode::parse(["--log-transform".to_owned()].into_iter()),
            Ok(ActivityLogMode::Transform)
        );
        assert!(
            ActivityLogMode::parse(
                ["--log-all".to_owned(), "--log-transform".to_owned()].into_iter()
            )
            .is_err()
        );
    }
}
