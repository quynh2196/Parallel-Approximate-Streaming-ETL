# môn học: tính toán song song
File: crawl_data lấy data -> được xử lý trực tiếp trong file ttss.py và ss.py mà không cần download. \n
Sử dụng Reservoir Sampling để lấy mẫu dữ liệu và Count-Min sketch để tính tần suất xuất hiện các từ. 
Số lượng bài báo lấy được trong thời gian "run_time" sẽ được lấy để xử lý. 
Data sẽ được cho vào input_queue, sau đó chia nhỏ vào các thread để xử lý, sau khi xử lý xong, data sẽ được cho vào output_queue để tính title_score. 
"best_title" sẽ được print liên tục nếu có 1 title khác có score cao hơn

