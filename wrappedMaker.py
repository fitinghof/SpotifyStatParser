import parser as pr
from matplotlib.backends.backend_pdf import PdfPages
from datetime import date
from pathlib import Path

# Settings
# The data period that should be counted
start_date = date(2010, 1, 1)
end_date = date(2030, 1, 1)

# Number of artist / songs to be featured on the top lists
nrof_top_artists = 5
nrof_top_songs = 20

# The windows for the rolling averages for the charts
listening_time_rolling_window = 31
top_songs_rolling_window = 31
top_artists_rolling_window = 31

# The pathes to look for streaming history and where to write the finished pdf
pdf_target_path = Path(".")
history_src_dir = Path("./Sebbe_streaming_history")

if __name__ == "__main__":
    wrapp = pr.WrappedMaker(
        start_date=start_date,
        end_date=end_date,
        pdf_target_path=pdf_target_path,
        history_src_dir=history_src_dir,
    )

    wrapp.front_page()
    wrapp.top_songs()
    wrapp.top_songs_chart()
    wrapp.top_artists()
    wrapp.top_artists_chart()
    wrapp.song_skip_stats()
    wrapp.least_skipped_top_songs(10)
    wrapp.play_time_chart()
    wrapp.play_time_per_hour_in_day()
    wrapp.play_time_per_weekday()
    wrapp.device_listening_time()
    wrapp.device_listening_chart()
    wrapp.write_to_file()