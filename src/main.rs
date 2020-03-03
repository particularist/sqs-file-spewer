extern crate futures;
extern crate rusoto_core;
extern crate rusoto_sqs;
extern crate rusoto_sts;
extern crate tokio_core;

#[macro_use]
extern crate quicli;

use uuid::Uuid;
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
    let (s, r) = mpsc::channel();
    thread::spawn(move || {
        lines.as_slice().chunks(10).for_each(|l| match s.send(l.to_owned()) {
            Ok(_) => {},
            Err(_) =>  println!("Receiver has stopped listening")
        })
    });

    let mut core = Core::new().unwrap();
    let q_url = "http://localhost:4576/queue/NewQueue".to_owned();

    loop {
        match r.recv() {
            Ok(lns) => match core.run(client.send_message_batch(sqs_send_message_batch_req(lns, &q_url))) {
                Ok(r) => println!("Success! {:?}",r),
                Err(e) => panic!("huh...{:?}", e)
            },
            Err(_) => panic!("Error occurred receiving lines")
        }
    }

   
}

fn batch_sqs_send(req: SendMessageBatchRequest, client: &SqsClient)

-> impl Future<Item = SendMessageBatchResult, Error = RusotoError<SendMessageBatchError>>
{
    client.send_message_batch(req)
}


fn message_body_to_smbre(body: String) -> SendMessageBatchRequestEntry {
    let mut se = SendMessageBatchRequestEntry::default();
    se.id = format!("{}", Uuid::new_v4());
    se.message_body = body;
    se
}

fn sqs_send_message_batch_req(msg_batch: Vec<String>, q_url: &String) -> SendMessageBatchRequest {
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
    let q_url = "http://test/";
    let mut core = Core::new().unwrap();
    let client = SqsClient::new(Region::Custom {
        name: "us-east-1".to_owned(),
        endpoint: "http://localhost:32834".to_owned(),
    });
    let lines: Vec<String> = io::BufReader::new(file).lines().map(|l| l.unwrap().to_string()).collect();
    lines.as_slice().chunks(10).for_each(|l| match core.run(batch_sqs_send(sqs_send_message_batch_req(l.to_owned(), q_url.to_owned()), &client)) {
        Ok(s) => println!("Found this {:?}",s),
        Err(e) => panic!("Error completing futures: {}", e),
    });
    //
    // let r = ListQueuesRequest::default();
    // match core.run(client.list_queues(r)) {
    //     Ok(s) => println!("Found this{:?}", s),
    //     Err(e) => panic!("Ahh! {}", e)
    // }
}

