use jetstreamer::JetstreamerRunner;
use jetstreamer_plugin::plugins::program_tracking::AccountSlotTrackingPlugin;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(JetstreamerRunner::default()
        .with_log_level("info")
        .parse_cli_args()?
        .with_plugin(Box::new(AccountSlotTrackingPlugin))
        .with_jetstreamer_geyser_config()
        .run()?)
}
