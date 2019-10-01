extern crate rusoto_core;
extern crate rusoto_sqs;
extern crate rusoto_sts;
extern crate tokio_core;

#[macro_use]
extern crate quicli;

use rusoto_sqs::{ SendMessageBatchRequest, SendMessageBatchRequestEntry };
use rusoto_core::credential::ProfileProvider;
use rusoto_core::{ HttpClient, Region };
use rusoto_sqs::{SqsClient, Sqs, ListQueuesRequest, ListQueuesResult};
use tokio_core::reactor::Core;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use quicli::prelude::*;

#[derive(Debug, StructOpt)]
struct Cli {
    #[structopt(long="filename", short="f")]
    filename: String,
    #[structopt(long="batch_size", short="b", default_value="10")]
    batch_size: usize,
    #[structopt(long="profile", short="p")]
    profile: String
}

fn get_credentials(profile: &str) -> ProfileProvider {
    let mut credentials = ProfileProvider::new().unwrap();
    if !profile.is_empty(){
        credentials.set_profile(profile);
    }

    credentials
}

fn print_queue_urls(res: ListQueuesResult){
    match res.queue_urls {
        Some(us) => for u in us.iter() {
            println!("{}",u)
        },
        None => println!("Nothing to see here"),
    }
}

main!(|args: Cli|{
    let region = Region::UsEast1;
    let client = SqsClient::new_with(HttpClient::new().unwrap(), get_credentials(&args.profile), region);
    let q_rq = ListQueuesRequest::default();
    let qs = client.list_queues(q_rq);
    let mut core = Core::new().unwrap();
    match core.run(qs) {
        Ok(q_urls) => print_queue_urls(q_urls),
        Err(e) => panic!("Nothing to see here:{}", e),
    }
});

fn message_body_to_smbre(body: &String) -> SendMessageBatchRequestEntry {
    let mut se = SendMessageBatchRequestEntry::default();
    se.message_body = body.to_owned();
    se
}

fn sqs_send_message_batch_req(msg_batch: &Vec<String>, q_url: &String) -> SendMessageBatchRequest {
   let mut req = SendMessageBatchRequest::default();
   req.queue_url = q_url.to_owned();
   let entries: Vec<SendMessageBatchRequestEntry> = msg_batch.into_iter().map(|m| message_body_to_smbre(m)).collect();
   req.entries = entries;
   req
}

#[test]
fn batch_messages_test() {
    let test_message_file_name = &"tests/resources/test_messages.txt";
    let file = match File::open(test_message_file_name) {
        Err(e) => panic!("Could not open {}: {}", test_message_file_name, e),
        Ok(f) => f,
    };
    let q_url = "http://test/".to_string();
    let mut v: Vec<String> = vec![];
    let batch_size = 5;
    {
        for l in io::BufReader::new(file).lines() {
            if &v.len() < &batch_size {
                match l {
                    Ok(line) => v.push(line.clone()),
                    Err(e) => panic!("Could not grab line {}", e),
                }
            }else{
                //Preparing batch
                let msg_batch = sqs_send_message_batch_req(&v, &q_url);
                assert_eq!(msg_batch.entries.len(), 5);
                assert_eq!(msg_batch.queue_url, q_url);
                msg_batch.entries.iter().for_each(|e| assert_ne!(e.message_body, ""));
                v.clear()
            }
        }
    }
}

