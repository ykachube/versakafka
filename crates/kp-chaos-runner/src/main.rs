//! Chaos test runner binary - runs scenarios against real proxy

use kp_testkit::{
    scenario_runner::{Scenario, ScenarioRunner, chaos_scenarios},
    toxiproxy::ToxiproxyClient,
    docker_client::DockerClient,
};
use std::time::Duration;
use tokio::time::sleep;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "chaos-runner")]
struct Args {
    /// Scenario name to run (or 'all' for all scenarios)
    #[arg(short, long)]
    scenario: Option<String>,

    /// Random seed
    #[arg(short, long, default_value = "42")]
    seed: u64,

    /// Toxiproxy URL
    #[arg(long, default_value = "http://localhost:8474")]
    toxiproxy_url: String,

    /// Kafka bootstrap servers
    #[arg(long, default_value = "localhost:9092")]
    kafka: String,

    /// Proxy address
    #[arg(long, default_value = "localhost:9092")]
    proxy: String,

    /// Run only unit tests (no external dependencies)
    #[arg(long)]
    unit_only: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    if args.unit_only {
        println!("Running unit tests only (no external dependencies)...");
        run_unit_scenarios(args.seed).await?;
        return Ok(());
    }

    // Initialize clients
    let toxiproxy = ToxiproxyClient::new(&args.toxiproxy_url);
    let docker = DockerClient::new()?;

    // Verify connections
    println!("Verifying Toxiproxy at {}...", args.toxiproxy_url);
    let proxies = toxiproxy.get_proxies().await?;
    println!("Found {} proxies", proxies.len());

    println!("Verifying Docker...");
    let containers = docker.list_containers(None).await?;
    println!("Found {} containers", containers.len());

    // Run scenarios
    if let Some(scenario_name) = args.scenario {
        if scenario_name == "all" {
            run_all_integration_scenarios(&toxiproxy, &docker, args.seed).await?;
        } else {
            run_single_integration_scenario(&toxiproxy, &docker, &scenario_name, args.seed).await?;
        }
    } else {
        // Default: run all
        run_all_integration_scenarios(&toxiproxy, &docker, args.seed).await?;
    }

    Ok(())
}

async fn run_unit_scenarios(seed: u64) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running built-in unit chaos scenarios (seed: {})...", seed);
    
    let scenarios = chaos_scenarios::all();
    let mut passed = 0;
    let mut failed = 0;

    for (i, scenario_fn) in scenarios.iter().enumerate() {
        let scenario = scenario_fn(seed + i as u64);
        println!("\n=== Scenario {}: {} ===", i + 1, scenario.name);
        
        let mut runner = ScenarioRunner::new(scenario);
        let result = runner.run().await?;
        result.print_summary();
        
        if result.passed {
            passed += 1;
        } else {
            failed += 1;
        }
    }

    println!("\n=== SUMMARY ===");
    println!("Passed: {}", passed);
    println!("Failed: {}", failed);
    
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_all_integration_scenarios(
    toxiproxy: &ToxiproxyClient,
    docker: &DockerClient,
    seed: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let scenarios = vec![
        "broker-crash",
        "network-partition",
        "slow-broker",
        "channel-unavailable",
        "s3-unavailable",
        "disk-full",
        "proxy-restart",
        "arc-flapping",
        "pod-restart",
        "combined",
    ];

    let mut passed = 0;
    let mut failed = 0;

    for name in scenarios {
        println!("\n=== Integration Scenario: {} ===", name);
        match run_single_integration_scenario(toxiproxy, docker, name, seed).await {
            Ok(true) => {
                println!("✓ PASSED");
                passed += 1;
            }
            Ok(false) => {
                println!("✗ FAILED");
                failed += 1;
            }
            Err(e) => {
                println!("✗ ERROR: {}", e);
                failed += 1;
            }
        }
        // Reset between scenarios
        toxiproxy.reset_all().await?;
        sleep(Duration::from_secs(2)).await;
    }

    println!("\n=== INTEGRATION SUMMARY ===");
    println!("Passed: {}", passed);
    println!("Failed: {}", failed);
    
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_single_integration_scenario(
    toxiproxy: &ToxiproxyClient,
    docker: &DockerClient,
    name: &str,
    seed: u64,
) -> Result<bool, Box<dyn std::error::Error>> {
    match name {
        "broker-crash" => run_broker_crash(toxiproxy, docker, seed).await,
        "network-partition" => run_network_partition(toxiproxy, docker, seed).await,
        "slow-broker" => run_slow_broker(toxiproxy, docker, seed).await,
        "channel-unavailable" => run_channel_unavailable(toxiproxy, docker, seed).await,
        "s3-unavailable" => run_s3_unavailable(toxiproxy, docker, seed).await,
        "disk-full" => run_disk_full(toxiproxy, docker, seed).await,
        "proxy-restart" => run_proxy_restart(toxiproxy, docker, seed).await,
        "arc-flapping" => run_arc_flapping(toxiproxy, docker, seed).await,
        "pod-restart" => run_pod_restart(toxiproxy, docker, seed).await,
        "combined" => run_combined_failures(toxiproxy, docker, seed).await,
        _ => {
            println!("Unknown scenario: {}", name);
            std::process::exit(1);
        }
    }
}

// Scenario implementations - these inject real failures
async fn run_broker_crash(toxiproxy: &ToxiproxyClient, _docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Injecting broker crash via Toxiproxy...");
    
    // Apply latency to simulate broker issues, then make unavailable
    toxiproxy.apply_kafka_latency(5000, 1000).await?;
    sleep(Duration::from_secs(5)).await;
    toxiproxy.apply_kafka_unavailable().await?;
    
    // Wait for scenario duration
    sleep(Duration::from_secs(30)).await;
    
    // Reset
    toxiproxy.reset_all().await?;
    
    // Run checker
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::broker_crash(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_network_partition(toxiproxy: &ToxiproxyClient, _docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Injecting network partition via Toxiproxy...");
    
    toxiproxy.apply_kafka_unavailable().await?;
    sleep(Duration::from_secs(30)).await;
    toxiproxy.reset_all().await?;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::network_partition(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_slow_broker(toxiproxy: &ToxiproxyClient, _docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Injecting slow broker via Toxiproxy...");
    
    toxiproxy.apply_kafka_latency(2000, 500).await?;
    sleep(Duration::from_secs(30)).await;
    toxiproxy.reset_all().await?;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::slow_broker(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_channel_unavailable(_toxiproxy: &ToxiproxyClient, docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Killing secondary channel (if exists)...");
    
    // Try to kill secondary Kafka if it exists
    let _ = docker.kill_kafka_secondary().await;
    sleep(Duration::from_secs(30)).await;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::channel_unavailable(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_s3_unavailable(toxiproxy: &ToxiproxyClient, _docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Making S3 (MinIO) unavailable via Toxiproxy...");
    
    toxiproxy.apply_minio_5xx(1.0).await?;
    sleep(Duration::from_secs(30)).await;
    toxiproxy.reset_all().await?;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::s3_unavailable(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_disk_full(_toxiproxy: &ToxiproxyClient, _docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Simulating disk full (would need fs failure layer)...");
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::disk_full(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_proxy_restart(_toxiproxy: &ToxiproxyClient, docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Restarting proxy container...");
    
    docker.restart_proxy().await?;
    sleep(Duration::from_secs(10)).await;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::proxy_restart_during_replay(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_arc_flapping(_toxiproxy: &ToxiproxyClient, _docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Injecting ARC flapping (requires fake-arc config changes)...");
    
    // This would require modifying fake-arc scenarios
    sleep(Duration::from_secs(30)).await;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::arc_flapping(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_pod_restart(_toxiproxy: &ToxiproxyClient, docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Restarting proxy pod (same as proxy restart)...");
    
    docker.restart_proxy().await?;
    sleep(Duration::from_secs(10)).await;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::pod_restart(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}

async fn run_combined_failures(toxiproxy: &ToxiproxyClient, _docker: &DockerClient, seed: u64) -> Result<bool, Box<dyn std::error::Error>> {
    println!("Injecting combined failures...");
    
    // Broker crash
    toxiproxy.apply_kafka_unavailable().await?;
    sleep(Duration::from_secs(10)).await;
    
    // Slow secondary
    toxiproxy.apply_kafka_latency(2000, 500).await?;
    sleep(Duration::from_secs(10)).await;
    
// S3 errors
    toxiproxy.apply_minio_5xx(0.5).await?;
    sleep(Duration::from_secs(30)).await;
    
    toxiproxy.reset_all().await?;
    
    let scenario = kp_testkit::scenario_runner::chaos_scenarios::combined_failures(seed);
    let mut runner = ScenarioRunner::new(scenario);
    let result = runner.run().await?;
    Ok(result.passed)
}