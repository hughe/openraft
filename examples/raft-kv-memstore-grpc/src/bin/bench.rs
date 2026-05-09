//! Set-RPC benchmark for raft-kv-memstore-grpc.
//!
//! Issues `--count` sequential `Set` calls against `--addr`, recording
//! per-request latency, then prints min / mean / p50 / p90 / p99 / max.

use std::time::Duration;
use std::time::Instant;

use clap::Parser;
use raft_kv_memstore_grpc::protobuf::SetRequest;
use raft_kv_memstore_grpc::protobuf::app_service_client::AppServiceClient;

#[derive(Parser, Debug)]
#[command(version, about = "openraft kv-memstore-grpc Set benchmark")]
struct Opt {
    /// Server gRPC address, e.g. "10.42.1.5:5051".
    #[arg(long)]
    addr: String,

    /// Number of Set RPCs to perform.
    #[arg(long, default_value_t = 100)]
    count: usize,

    /// Key prefix; each request appends an index to this prefix.
    #[arg(long, default_value = "bench-")]
    key_prefix: String,

    /// Value written for every request.
    #[arg(long, default_value = "v")]
    value: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let opt = Opt::parse();

    let url = format!("http://{}", opt.addr);
    let mut client = AppServiceClient::connect(url).await?;

    let mut latencies: Vec<Duration> = Vec::with_capacity(opt.count);

    let bench_start = Instant::now();
    for i in 0..opt.count {
        let req = SetRequest {
            key: format!("{}{}", opt.key_prefix, i),
            value: opt.value.clone(),
        };
        let t0 = Instant::now();
        client.set(req).await?;
        latencies.push(t0.elapsed());
    }
    let total_elapsed = bench_start.elapsed();

    latencies.sort_unstable();
    let n = latencies.len();
    let sum: Duration = latencies.iter().sum();
    let mean = sum / n as u32;
    let percentile = |q: f64| -> Duration {
        let idx = ((n as f64 - 1.0) * q).round() as usize;
        latencies[idx]
    };
    let min = *latencies.first().unwrap();
    let max = *latencies.last().unwrap();
    let throughput = n as f64 / total_elapsed.as_secs_f64();

    println!("addr      : {}", opt.addr);
    println!("count     : {}", n);
    println!("total     : {:.3}s", total_elapsed.as_secs_f64());
    println!("throughput: {:.1} req/s", throughput);
    println!("min       : {:?}", min);
    println!("mean      : {:?}", mean);
    println!("p50       : {:?}", percentile(0.50));
    println!("p90       : {:?}", percentile(0.90));
    println!("p99       : {:?}", percentile(0.99));
    println!("max       : {:?}", max);

    Ok(())
}
