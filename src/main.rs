use tokio::fs::{File, OpenOptions, remove_file};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::cmp::Reverse;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "shakes.txt";
    let num_map_workers = 4;
    let num_reduce_workers = 4;
    let num_chunks = 4;

    // Read the file and split into chunks
    let chunks = split_file(file_path, num_chunks).await?;

    // Map phase
    let map_results = map_phase(&chunks, num_map_workers).await?;

    // Reduce phase
    let final_result = reduce_phase(map_results.clone(), num_reduce_workers).await?;

    // Write final result
    write_final_result(&final_result).await?;

    // Print top 10 words
    print_top_words(&final_result, 10);

    // Clean up intermediate files
    cleanup_intermediate_files(&map_results).await?;

    Ok(())
}

async fn split_file(file_path: &str, num_chunks: usize) -> Result<Vec<String>, std::io::Error> {
    let file = File::open(file_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut chunks = vec![String::new(); num_chunks];
    let mut chunk_index = 0;

    while let Some(line) = lines.next_line().await? {
        chunks[chunk_index].push_str(&line);
        chunks[chunk_index].push('\n');
        chunk_index = (chunk_index + 1) % num_chunks;
    }

    Ok(chunks)
}

async fn map_phase(chunks: &[String], num_workers: usize) -> Result<Vec<String>, std::io::Error> {
    let chunk_queue = Arc::new(Mutex::new(chunks.to_vec()));
    let results = Arc::new(Mutex::new(Vec::new()));

    let mut handles = vec![];

    for worker_id in 0..num_workers {
        let chunk_queue = Arc::clone(&chunk_queue);
        let results = Arc::clone(&results);

        let handle = tokio::spawn(async move {
            loop {
                let chunk = {
                    let mut queue = chunk_queue.lock().await;
                    queue.pop()
                };

                match chunk {
                    Some(text) => {
                        let word_counts = count_words(&text);
                        let result = format!("map_{}.txt", worker_id);
                        write_map_result(&result, &word_counts).await?;
                        results.lock().await.push(result);
                    }
                    None => break,
                }
            }
            Ok::<_, std::io::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(Arc::try_unwrap(results).unwrap().into_inner())
}

fn count_words(text: &str) -> HashMap<String, usize> {
    let mut word_counts = HashMap::new();
    for word in text.split_whitespace() {
        let word = word.to_lowercase();
        *word_counts.entry(word).or_insert(0) += 1;
    }
    word_counts
}

async fn write_map_result(file_name: &str, word_counts: &HashMap<String, usize>) -> Result<(), std::io::Error> {
    let mut file = File::create(file_name).await?;
    for (word, count) in word_counts {
        file.write_all(format!("{} {}\n", word, count).as_bytes()).await?;
    }
    Ok(())
}

async fn reduce_phase(map_results: Vec<String>, num_workers: usize) -> Result<HashMap<String, usize>, std::io::Error> {
    let result_queue = Arc::new(Mutex::new(map_results));
    let final_result = Arc::new(Mutex::new(HashMap::new()));

    let mut handles = vec![];

    for _ in 0..num_workers {
        let result_queue = Arc::clone(&result_queue);
        let final_result = Arc::clone(&final_result);

        let handle = tokio::spawn(async move {
            loop {
                let result_file = {
                    let mut queue = result_queue.lock().await;
                    queue.pop()
                };

                match result_file {
                    Some(file_name) => {
                        let word_counts = read_map_result(&file_name).await?;
                        let mut final_result = final_result.lock().await;
                        for (word, count) in word_counts {
                            *final_result.entry(word).or_insert(0) += count;
                        }
                    }
                    None => break,
                }
            }
            Ok::<_, std::io::Error>(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(Arc::try_unwrap(final_result).unwrap().into_inner())
}

async fn read_map_result(file_name: &str) -> Result<HashMap<String, usize>, std::io::Error> {
    let file = File::open(file_name).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut word_counts = HashMap::new();

    while let Some(line) = lines.next_line().await? {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 2 {
            if let Ok(count) = parts[1].parse::<usize>() {
                word_counts.insert(parts[0].to_string(), count);
            }
        }
    }

    Ok(word_counts)
}

async fn write_final_result(word_counts: &HashMap<String, usize>) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open("final_result.txt")
        .await?;

    for (word, count) in word_counts {
        file.write_all(format!("{} {}\n", word, count).as_bytes()).await?;
    }

    Ok(())
}

fn print_top_words(word_counts: &HashMap<String, usize>, n: usize) {
    let mut counts: Vec<_> = word_counts.iter().collect();
    counts.sort_by_key(|&(_, count)| Reverse(*count));

    println!("Top {} words:", n);
    for (word, count) in counts.iter().take(n) {
        println!("{}: {}", word, count);
    }
}

async fn cleanup_intermediate_files(map_results: &[String]) -> Result<(), std::io::Error> {
    for file_name in map_results {
        if let Err(e) = remove_file(file_name).await {
            eprintln!("Error deleting file {}: {}", file_name, e);
        }
    }
    Ok(())
}