extern crate futures;
extern crate rusoto_core;
extern crate rusoto_sqs;
extern crate rusoto_sts;
extern crate tokio_core;

#[macro_use]
extern crate quicli;

use std::thread;
use std::sync::mpsc;
use rusoto_sqs::{ SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageBatchResult, SendMessageBatchError };
use rusoto_core::credential::ProfileProvider;
use futures::future::Future;
use rusoto_core::{ HttpClient, Region, RusotoError};
use rusoto_sqs::{SqsClient, Sqs, ListQueuesRequest, ListQueuesResult};
use tokio_core::reactor::Core;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use rayon::prelude::*;
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
    let client = SqsClient::new(Region::Custom {
        name: "us-east-1".to_owned(),
        endpoint: "http://localhost:4576".to_owned(),
    });
    batchSubmit(args.filename, &client)
});


fn batchSubmit(filename: String, client: &SqsClient) {
    let file = match File::open(&filename) {
        Err(e) => panic!("Could not open {}: {}", &filename, e),
        Ok(f) => f,
    };
    let lines: Vec<String> = io::BufReader::new(file).lines().map(|l| l.unwrap().to_string()).collect(); 
    
    let q_url = "http://localhost:4576/queue/NewQueue".to_owned();
    let (s, r) = mpsc::channel();
    thread::spawn(move || {
        lines.as_slice().chunks(10).for_each(|l| match s.send(l.to_owned()) {
            Ok(_) => {},
            Err(_) =>  println!("Receiver has stopped listening")
        })
    });

    loop{
        match r.recv() {
            Ok(lns) => println!("{:?}", lns),
            Err(_) => panic!("Error occurred receiving lines")
        }
    }

   
}

fn batch_sqs_send(req: SendMessageBatchRequest, client: &SqsClient)

-> impl Future<Item = SendMessageBatchResult, Error = RusotoError<SendMessageBatchError>>
{
    client.send_message_batch(req)
}


fn message_body_to_smbre(body: &String) -> SendMessageBatchRequestEntry {
    let mut se = SendMessageBatchRequestEntry::default();
    se.message_body = body.to_owned();
    se
}

fn sqs_send_message_batch_req(msg_batch: &Vec<std::result::Result<String, std::io::Error>>, q_url: &String) -> SendMessageBatchRequest {
   let mut req = SendMessageBatchRequest::default();
   req.queue_url = q_url.to_owned();
   let entries: Vec<SendMessageBatchRequestEntry> = msg_batch.into_iter().map(|m| message_body_to_smbre(m.as_ref().unwrap())).collect();
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
    let mut core = Core::new().unwrap();
    let mut v: Vec<&String> = vec![];
    let batch_size = 5;
    {
        for l in io::BufReader::new(file).lines() {
            if &v.len() < &batch_size {
                match l {
                    Ok(line) => v.push(&line.clone()),
                    Err(e) => panic!("Could not grab line {}", e),
                }
            }else{
                //Preparing batch
                let msg_batch = sqs_send_message_batch_req(&v.chunks(1), &q_url);
                assert_eq!(msg_batch.entries.len(), 5);
                assert_eq!(msg_batch.queue_url, q_url);
                msg_batch.entries.iter().for_each(|e| assert_ne!(e.message_body, ""));
                v.clear()
            }
        }
    }
}

