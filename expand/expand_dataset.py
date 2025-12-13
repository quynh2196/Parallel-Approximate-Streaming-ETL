# expand_dataset.py

INPUT_FILE  = "filtered_news_dataset.json"      # file 70k
OUTPUT_FILE = "filtered_news_dataset_2M.json"   # file 2 triệu
TARGET = 2_000_000


def is_valid_line(line: str) -> bool:
    return line.strip() != ""


def main():
    count = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as fin, \
         open(OUTPUT_FILE, "w", encoding="utf-8") as fout:

        while count < TARGET:
            line = fin.readline()

            # hết file → quay lại đầu (giả lập stream lặp)
            if not line:
                fin.seek(0)
                continue

            if not is_valid_line(line):
                continue

            fout.write(line)
            count += 1

            if count % 100_000 == 0:
                print(f"Written {count} records")

    print(f"Done. Total records written: {count}")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
