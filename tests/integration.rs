extern crate assert_cmd;
extern crate predicates;

use assert_cmd::prelude::*;
use std::process::Command;
use predicates::prelude::*;

#[test]
fn help_missing_filename_test() {
    let expected_err_preamble = "error: The following required arguments were not provided:
    --filename <filename>";
    let cmd = Command::cargo_bin("sqs-file-spewer").unwrap().assert()
        .stderr(predicate::str::contains(expected_err_preamble));
}
