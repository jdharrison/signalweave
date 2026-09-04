#![deny(unsafe_code)]

use woven_loadtest::{LoadConfig, Scenario, run};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_config(std::env::args().skip(1))?;
    let measurement = run(config)?;
    println!("scenario={}", measurement.scenario.name());
    println!("participants={}", measurement.participants);
    println!("attempted_publishes={}", measurement.attempted_publishes);
    println!("delivered_messages={}", measurement.delivered_messages);
    println!("elapsed_ms={}", measurement.elapsed.as_millis());
    println!(
        "publishes_per_second={:.2}",
        measurement.publishes_per_second()
    );
    println!(
        "p50_publish_latency_us={}",
        micros(measurement.p50_publish_latency)
    );
    println!(
        "p95_publish_latency_us={}",
        micros(measurement.p95_publish_latency)
    );
    println!(
        "p99_publish_latency_us={}",
        micros(measurement.p99_publish_latency)
    );
    println!(
        "peak_pending_messages={}",
        measurement.peak_pending_messages
    );
    println!("latest_replacements={}", measurement.latest_replacements);
    println!("latest_drops={}", measurement.latest_drops);
    println!("best_effort_drops={}", measurement.best_effort_drops);
    println!(
        "logical_cpus={}",
        measurement
            .logical_cpus
            .map_or_else(|| "unavailable".to_owned(), |count| count.to_string())
    );
    println!("operating_system={}", measurement.operating_system);
    println!("architecture={}", measurement.architecture);
    Ok(())
}

fn parse_config(mut arguments: impl Iterator<Item = String>) -> Result<LoadConfig, String> {
    let mut config = LoadConfig::default();
    while let Some(value) = arguments.next() {
        let next = arguments
            .next()
            .ok_or_else(|| format!("missing value for {value}"))?;
        match value.as_str() {
            "--scenario" => {
                config.scenario =
                    Scenario::parse(&next).ok_or_else(|| format!("unknown scenario: {next}"))?;
            }
            "--participants" => {
                config.participants = next
                    .parse()
                    .map_err(|_| format!("invalid participant count: {next}"))?;
            }
            "--rounds" => {
                config.rounds = next
                    .parse()
                    .map_err(|_| format!("invalid round count: {next}"))?;
            }
            "--max-latency-samples" => {
                config.max_latency_samples = next
                    .parse()
                    .map_err(|_| format!("invalid sample count: {next}"))?;
            }
            _ => return Err(format!("unknown argument: {value}")),
        }
    }
    Ok(config)
}

fn micros(value: Option<std::time::Duration>) -> String {
    value.map_or_else(
        || "unavailable".to_owned(),
        |duration| duration.as_micros().to_string(),
    )
}
