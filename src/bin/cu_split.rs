use anyhow::{anyhow, bail, Result as AResult};
use async_std::fs::File as AFile;
use async_std::path::PathBuf as APathBuf;
use async_std::prelude::*;
use async_std::task::{block_on, spawn as a_spawn};
use clap::{App, Arg};
use csv::{Reader, StringRecord};
use std::ffi::OsStr;
use std::process::Command;

fn main() -> AResult<()> {
    block_on(async_main())
}

async fn async_main() -> AResult<()> {
    let args = App::new("split_rush")
        .arg(Arg::with_name("INFO").required(true))
        .arg(Arg::with_name("VIDEO"))
        .get_matches_safe()?;
    let si_file = APathBuf::from(args.value_of("INFO").ok_or(anyhow!("no_info"))?);
    let vid_file = if let Some(v) = args.value_of("VIDEO") {
        APathBuf::from(v)
    } else {
        si_file.with_file_name(
            si_file
                .file_stem()
                .ok_or(anyhow!("cannot infer video name"))?,
        )
    };
    let mut si_data = Vec::new();
    AFile::open(si_file)
        .await?
        .read_to_end(&mut si_data)
        .await?;
    let mut rdr = Reader::from_reader(si_data.as_slice());
    let mut futures = Vec::new();
    for record in rdr.records() {
        let split_info = parse_si(record?)?;
        futures.push(a_spawn(work(split_info, vid_file.clone())));
    }
    for future in futures {
        if !future.await? {
            bail!("some ffmpeg invocation failed");
        }
    }
    Ok(())
}

fn o(v: &str) -> &OsStr {
    OsStr::new(v)
}

fn b(v: Option<&OsStr>) -> AResult<&str> {
    Ok(v.ok_or(anyhow!("bad filename"))?
        .to_str()
        .ok_or(anyhow!("bad filename"))?)
}

async fn work(si: SplitInfo, vid: APathBuf) -> AResult<bool> {
    let out = vid.with_file_name(format!(
        "{inf}_split_{num:04}.{ext}",
        inf = b(vid.file_stem())?,
        ext = b(vid.extension())?,
        num = si.number
    ));
    let status = Command::new("ffmpeg")
        .args(&[
            o("-i"),
            vid.as_os_str(),
            o("-ss"),
            o(&si.start),
            o("-to"),
            o(&si.end),
            o("-c"),
            o("copy"),
            out.as_os_str(),
        ])
        .status()?;
    Ok(status.success())
}

fn parse_si(r: StringRecord) -> AResult<SplitInfo> {
    let mut iter = r.iter();
    Ok(SplitInfo {
        number: r.position().ok_or(anyhow!("unknown split number"))?.line(),
        start: iter.next().ok_or(parse_si_error(&r, "start"))?.to_string(),
        end: iter.next().ok_or(parse_si_error(&r, "end"))?.to_string(),
    })
}

fn parse_si_error(r: &StringRecord, pos: &str) -> anyhow::Error {
    return match r.position() {
        Some(p) => anyhow!("missing {} for record {}", pos, p.line()),
        None => anyhow!("missing {} for some unknown record", pos),
    };
}

struct SplitInfo {
    number: u64,
    start: String,
    end: String,
}
