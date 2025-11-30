import time
import multiprocessing as mp
from crawl_data import main
from ttss import (CountMinSketch, clean_and_tokenize, merge_multiple_cms, compute_title_score, find_title)
import random
import json

def worker(input_queue, output_queue):
    cms = CountMinSketch()
    num_processed = 0
    while True:
        batch = input_queue.get()
        if batch == "DONE":
            break
        for item in batch:  # xử lý từng item trong batch
            title = item["title"]
            for t in clean_and_tokenize(title):
                cms.add(t)
            num_processed += 1
    output_queue.put((cms, num_processed))

if __name__ == "__main__":
    # lay data trong run_time seconds --> lay cung 1 data cho dong deu
    run_time =4 # seconds
    def stream_from_json(path):
        while True: 
            data = json.load(open(path, "r", encoding="utf-8"))
            for item in data:
                yield item

    stream = stream_from_json("filtered_news_dataset.json")    

    fixed_data = []
    start = time.time()

    k = 2162005
    reservoir = [] #reservoir sampling
    count = 0
    while time.time() - start < run_time:
        item = next(stream)
        count += 1
        if count <= k:
            reservoir.append(item)
        else:
            j = random.randint(0, count - 1)
            if j < k:
                reservoir[j] = item

    fixed_data = reservoir
    base=0
    print(f"length = {len(fixed_data)}")
    for n_threads in [1, 2, 4, 8, 12, 16, 20]:
        print(f"\n {n_threads} threads")
        input_queue = mp.Queue()
        output_queue = mp.Queue()
        procs = [
            mp.Process(target=worker, args=(input_queue, output_queue))
            for _ in range(n_threads)
        ]

        for p in procs:
            p.start()

        batch_size = 100
        batch = []
        for item in fixed_data:
            batch.append(item)
            if len(batch) >= batch_size:
                input_queue.put(batch)
                batch = []
        if batch:
            input_queue.put(batch) # day toan bo data vao input_queue
            
        for _ in range(n_threads):
            input_queue.put("DONE")

        start_time = time.time()
        cms_list = []
        total_processed = 0
        for _ in range(n_threads):
            cms_part, cnt = output_queue.get()
            cms_list.append(cms_part)
            total_processed += cnt

        final_cms = merge_multiple_cms(cms_list)
        best_title = find_title(fixed_data, cms_list)
        best_score = compute_title_score(final_cms, best_title)

        for p in procs:
            p.join()
        end_time = time.time()
        if n_threads == 1:
            base=end_time - start_time
        
        print(f"Processed: {total_processed} articles")
        print(f"Best title: {best_title}  (score={best_score})")
        print(f"Time: {(end_time - start_time)*1000:.2f} ms")
        print(f"speed: {base/(end_time-start_time)}")


