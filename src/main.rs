
use clap::Parser;
use rdkafka::{config::RDKafkaLogLevel, consumer::{Consumer, ConsumerContext, StreamConsumer}, util::TokioRuntime, ClientConfig, ClientContext, Message};


struct CustomContext {

}


impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
}



#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    bootstrap: String,

    #[arg(short, long)]
    topic: String,

    #[arg(short, long)]
    username: String,

    #[arg(short, long)]
    password: String,
}


#[tokio::main]
async fn main() -> Result<(), ()> {

    let Args { bootstrap, username, password, topic, ..}  = Args::parse();

    let context = CustomContext {};
    let consumer: StreamConsumer<CustomContext, TokioRuntime> = ClientConfig::new()
    .set("group.id", format!("test-consumer"))
    .set("bootstrap.servers", bootstrap)
    .set("enable.partition.eof", "false")
    .set("session.timeout.ms", "10000")
    .set("max.poll.interval.ms", "600000")
     
  //  .set("enable.auto.commit", "true")
    .set("enable.auto.commit", "false")
    .set("security.protocol", "SASL_PLAINTEXT")
    .set("sasl.mechanism", "PLAIN")
    .set("sasl.username", username)
    .set("sasl.password", password)
    
    .set("heartbeat.interval.ms", "1000")
    .set("fetch.min.bytes", "1048576")
    .set("max.partition.fetch.bytes", "10485760")
    .set("fetch.max.bytes", "10485760")
    
    .set("auto.offset.reset", "earliest")
    .set_log_level(RDKafkaLogLevel::Info)
    .create_with_context(context)
    .expect("Consumer creation failed");

    consumer.subscribe(&[topic.as_str()]).expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Ok(value) => {
                let key = value.key().map(|key| { String::from_utf8(key.to_vec()).unwrap_or(String::from("NO KEY")) } ).unwrap();
                let payload = value.payload().map(|payload| {
                    String::from_utf8(payload.to_vec()).unwrap()
                });
                println!("NEW MESSAGE  KEY: {} PARTITION {} PAYLOAD: {:?}", key, value.partition(), payload)
            },

            Err(e) => {
                println!("Error {:?}", e.rdkafka_error_code());
            }
        }

    }
}
