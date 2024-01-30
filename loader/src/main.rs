use anyhow::{bail, Result};
use rand::distributions::Distribution;
use scylla::{query::Query, SessionBuilder};
use std::{borrow::BorrowMut, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = 1000)]
    request_concurrency: u64,

    #[arg(long, default_value_t = 4000000)]
    partition_concurrency: u64,

    #[arg(long)]
    host: String,

    #[arg(long)]
    partitions: u64,
}

async fn inner_main(idx: usize, args: &Args) -> Result<()> {
    let session = Arc::new(SessionBuilder::new().known_node(&args.host).build().await?);

    session.query(Query::new("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}"), &[]).await?;
    session.query(Query::new("CREATE TABLE IF NOT EXISTS ks.t (pk bigint, ck bigint, primary key (pk, ck))"), &[]).await?;

    let insert_stmt = Arc::new(
        session
            .prepare("INSERT INTO ks.t (pk, ck) VALUES (?, ?)")
            .await?
    );
    let select_stmt = Arc::new(
        session
            .prepare("SELECT ck FROM ks.t WHERE pk = ?")
            .await?
    );
    
    let request_sem = Arc::new(Semaphore::new(args.request_concurrency as usize));
    let partition_sem = Arc::new(Semaphore::new(args.partition_concurrency as usize));

    for pk in (idx as i64)*1_000_000_000_000..args.partitions as i64 + (idx as i64)*1_000_000_000_000 {
        let session = session.clone();
        let request_sem = request_sem.clone();
        let req_perm = request_sem.clone().acquire_owned().await;
        let partition_permit = partition_sem.clone().acquire_owned().await;
        let insert_stmt = insert_stmt.clone();
        let select_stmt = select_stmt.clone();
        tokio::task::spawn(async move {
            let _partition_permit = partition_permit;
            let _req_perm = req_perm;
            {
                let mut inserts = Vec::new();
                let _perm = request_sem.acquire_many(10).await;
                for ck in 0..10 as i64 {
                    inserts.push(session.execute(&insert_stmt, (pk, ck)));
                }
                let res: Result<Vec<_>> = futures::future::join_all(inserts).await.into_iter().map(|x| Ok(x?)).collect();
                res.unwrap();
            }
            std::mem::drop(_req_perm);
            let sleep_ms = rand::distributions::Uniform::new_inclusive(500, 4000).sample(rand::thread_rng().borrow_mut());
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            {
                let mut selects = Vec::new();
                let n_selects = rand::distributions::Uniform::new_inclusive(1, 3).sample(rand::thread_rng().borrow_mut());
                for _ in 0..n_selects {
                    let session = session.clone();
                    let request_sem = request_sem.clone();
                    let select_stmt = select_stmt.clone();
                    selects.push(tokio::task::spawn(async move {
                        let _perm = request_sem.acquire_owned().await;
                        let res = session.execute(&select_stmt, (pk,)).await;
                        let rn = res?.rows_num()?;
                        if rn != 10 {
                            bail!(format!("Bad row count: pk:{} count:{} n_selects:{} sleep_ms:{}", pk, rn, n_selects, sleep_ms))
                        }
                        Ok(())
                    }));
                }
                let res: Result<Vec<_>> = futures::future::join_all(selects).await.into_iter().map(|x| Ok(x??)).collect();
                res.unwrap();
            }
        });
        tokio::task::yield_now().await;
    }
    partition_sem.acquire_many(args.partition_concurrency as u32).await?.forget();
    Ok(())
}

fn main() {
    use clap::Parser;
    let args = Args::parse();
    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();
    pool.broadcast(|ctx| {
        let run = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        run.block_on(inner_main(ctx.index(), &args)).unwrap()
    });
}
