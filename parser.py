import pandas as pd
import matplotlib.pyplot as plt
import japanize_matplotlib
import re
from pathlib import Path
from datetime import date, timedelta
from matplotlib.backends.backend_pdf import PdfPages


class WrappedMaker:
    # -------------
    __df: pd.DataFrame
    __nrof_top_artist: int
    __nrof_top_songs: int
    __play_time_rolling: int
    __top_songs_rolling: int
    __top_artists_rolling: int
    __pdf_pages: PdfPages

    __top_songs: pd.DataFrame = None
    __top_artists: pd.DataFrame = None

    def __init__(
        self,
        start_date: date = date(1, 1, 1),
        end_date: date = date(4000, 12, 31),
        nrof_top_artist: int = 5,
        nrof_top_songs: int = 10,
        play_time_rolling: int = 7,
        top_songs_rolling: int = 7,
        top_artists_rolling: int = 7,
        pdf_target_path: Path = Path("."),
        history_src_dir: Path = Path("./StreamingHistory"),
    ) -> pd.DataFrame:
        self.__df = pd.DataFrame()
        directory = Path(history_src_dir)
        pattern = re.compile(r"StreamingHistory_music_\d+\.json")

        for file_path in directory.iterdir():
            if file_path.is_file() and pattern.match(file_path.name):
                self.__df = pd.concat([self.__df, pd.read_json(file_path)])
        self.__df = self.__df.drop_duplicates()

        self.__df["endTime"] = pd.to_datetime(self.__df["endTime"])
        self.__df["msPlayed"] = pd.to_numeric(self.__df["msPlayed"])

        self.__start_date = max(start_date, self.__df["endTime"].min().date())
        self.__end_date = min(end_date + timedelta(days=1), self.__df["endTime"].max().date() + timedelta(days=1))

        self.__df = self.__df.query(
            f"endTime >= '{self.__start_date}' & endTime <= '{self.__end_date}'"
        )

        self.__nrof_top_artist = nrof_top_artist
        self.__nrof_top_songs = nrof_top_songs
        self.__play_time_rolling = play_time_rolling
        self.__top_songs_rolling = top_songs_rolling
        self.__top_artists_rolling = top_artists_rolling
        self.__pdf_pages = PdfPages(Path.joinpath(pdf_target_path, "Wrapped.pdf"))

    # ----------------

    # Front page
    def fontPage(self) -> None:
        plt.figure(figsize=(16, 9))  # Standard letter size
        plt.text(0.5, 0.95, "Spotify Wrapped", fontsize=30, ha="center", va="top")
        period = self.__end_date - self.__start_date
        info_string = f"""Stats for period {self.__start_date} to {self.__end_date}\n
        Total listening time: {(self.__df["msPlayed"].sum() / 3600000).round(2)}h\n
        Average listening time per day: {(self.__df['msPlayed'].sum() / period.days / 3600000).round(2)}h\n"""

        plt.text(0.5, 0.85, info_string, fontsize=20, ha="center", va="top")
        plt.axis("off")  # Hide axes

        # Save the text page to the PDF
        self.__pdf_pages.savefig()
        plt.close()

    def __make_top_songs(self) -> None:
        top_songs = (
            self.__df.groupby(["artistName", "trackName"])["msPlayed"]
            .count()
            .sort_values(ascending=False)
            .rename("playCount")
            .head(self.__nrof_top_songs)
        )
        self.__top_songs = pd.DataFrame(top_songs).sort_values(by='playCount', ascending=True)

    def topSongs(self) -> None:
        if self.__top_songs is None:
            self.__make_top_songs()

        top_songs_df = self.__top_songs

        plt.figure(figsize=(16, 9))

        plt.barh(
            [str(i[1]) + " - " + str(i[0]) for i in top_songs_df.index],
            top_songs_df["playCount"],
            color="skyblue",
        )
        for index, value in enumerate(top_songs_df["playCount"]):
            plt.text(
                value / 2, index, f"{value}", ha="center", va="center", fontsize=15
            )

        plt.title(f"Top {self.__nrof_top_songs} songs", fontsize=20)
        plt.ylabel("Song Title", fontsize=15)
        plt.xlabel("Play Count", fontsize=15)

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self.__pdf_pages.savefig()
        self.__top_songs = top_songs_df

    def topSongsListenChart(self):
        if self.__top_songs is None:
            self.__make_top_songs()

        top_songs_full = self.__df[
            self.__df.set_index(["artistName", "trackName"]).index.isin(
                self.__top_songs.index
            )
        ]

        top_songs_daily = (
            top_songs_full.groupby(["artistName", "trackName"])
            .resample("d", on="endTime")
            .count()
        )
        top_songs_daily["playCount"] = top_songs_daily["artistName"]
        top_songs_daily = top_songs_daily.drop(
            ["artistName", "trackName", "msPlayed"], axis=1
        )

        plt.figure(figsize=(16, 9))

        for artist, track in reversed(self.__top_songs.index):
            song_data = top_songs_daily.query(
                f"artistName == '{artist}' & trackName == '{track}'"
            ).reset_index()
            song_data["rolling_playCount"] = (
                song_data["playCount"].rolling(window=self.__top_songs_rolling).mean()
            )
            plt.plot(
                song_data["endTime"], song_data["rolling_playCount"], label=f"{track}"
            )

        plt.legend(fontsize=15)
        plt.title(
            f"Top {self.__nrof_top_songs} Songs listening time rolling {self.__top_songs_rolling} day average",
            fontsize=20,
        )
        plt.xlabel("Date", fontsize=15)
        plt.ylabel("Play Count", fontsize=15)
        plt.grid()

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self.__pdf_pages.savefig()

    def __make_top_artist(self) -> None:
        top_artists = (
            self.__df.groupby(["artistName"])["msPlayed"]
            .count()
            .sort_values(ascending=False)
            .rename("playCount")
            .head(self.__nrof_top_artist)
        )
        self.__top_artists = pd.DataFrame(top_artists).sort_values(by='playCount', ascending=True)

    def topArtists(self) -> None:

        if self.__top_artists is None:
            self.__make_top_artist()

        top_artist_df = self.__top_artists

        plt.figure(figsize=(16, 9))

        top_artist_df = top_artist_df.sort_values(by="playCount", ascending=True)

        plt.barh(top_artist_df.index, top_artist_df["playCount"], color="skyblue")
        for index, value in enumerate(top_artist_df["playCount"]):
            plt.text(
                value / 2, index, f"{value}", ha="center", va="center", fontsize=15
            )

        plt.title(f"Top {self.__nrof_top_artist} artists", fontsize=20)
        plt.ylabel("Artist name", fontsize=15)
        plt.xlabel("Play Count", fontsize=15)

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self.__pdf_pages.savefig()

        self.__top_artists = top_artist_df

    def topArtistsChart(self):
        if self.__top_artists is None:
            self.__make_top_artist()

        top_artists_full = self.__df[
            self.__df.set_index(["artistName"]).index.isin(self.__top_artists.index)
        ]

        top_artists_daily = (
            top_artists_full.groupby(["artistName"]).resample("D", on="endTime").count()
        )
        top_artists_daily["playCount"] = top_artists_daily["artistName"]
        top_artists_daily = top_artists_daily.drop(
            ["artistName", "trackName", "msPlayed"], axis=1
        )

        plt.figure(figsize=(16, 9))

        for artist in reversed(self.__top_artists.index):
            song_data = top_artists_daily.query(
                f"artistName == '{artist}'"
            ).reset_index()
            song_data["rolling_playcount"] = (
                song_data["playCount"].rolling(window=self.__top_artists_rolling).mean()
            )
            plt.plot(
                song_data["endTime"], song_data["rolling_playcount"], label=f"{artist}"
            )

        plt.title(
            f"Top {self.__nrof_top_artist} artists listening time rolling {self.__top_artists_rolling} day average",
            fontsize=20,
        )

        plt.legend(fontsize=15)
        plt.xlabel("Date", fontsize=15)
        plt.ylabel("Play Count", fontsize=15)
        plt.grid()

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self.__pdf_pages.savefig()

    def playTimeChart(self):

        playtime = self.__df.resample("D", on="endTime")["msPlayed"].sum()

        # Convert to hours
        playtime = playtime.divide(3600000)

        playtime = playtime.rolling(window=self.__play_time_rolling).mean()

        plt.figure(figsize=(16, 9))
        plt.plot(playtime)

        plt.title(
            f"Total playtime rolling {self.__play_time_rolling} day average",
            fontsize=20,
        )
        plt.xlabel("Date", fontsize=15)
        plt.ylabel("Play Time (h)", fontsize=15)

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.grid()

        plt.tight_layout()
        self.__pdf_pages.savefig()

    def playTimePerHourInDay(self):

        ms_per_hour = self.__df.copy()
        ms_per_hour["hours"] = self.__df["endTime"].dt.hour
        ms_per_hour = ms_per_hour.groupby("hours")["msPlayed"].sum()
        ms_per_hour = ms_per_hour.reindex(range(24), fill_value=0)
        total_listening_time = ms_per_hour.sum()
        percent_occurrences_per_hour = (ms_per_hour / total_listening_time) * 100

        plt.figure(figsize=(16, 9))
        plt.bar(range(24), percent_occurrences_per_hour, color="skyblue")
        for index, value in enumerate(percent_occurrences_per_hour):
            formatedValue = ""
            if value > 1:
                formatedValue = f"{round(value, 2)}%"
            plt.text(
                index,
                value / 2,
                formatedValue,
                ha="center",
                va="center",
                fontsize=15,
                rotation=90,
            )

        plt.title("Listening spread per hour of the day", fontsize=20)
        plt.xlabel("Hour of the Day", fontsize=15)
        plt.ylabel("Percent of listening time", fontsize=15)

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)
        plt.xticks(rotation=0)

        plt.tight_layout()
        self.__pdf_pages.savefig()

    def playTimePerWeekday(self):
        listen_time_weekday = self.__df.copy()
        listen_time_weekday["weekday"] = self.__df["endTime"].dt.weekday
        listen_time_weekday = listen_time_weekday.groupby("weekday")["msPlayed"].sum()
        listen_time_weekday = listen_time_weekday.reindex(range(7), fill_value=0)

        total_listening_time = listen_time_weekday.sum()
        percent_occurrences_per_hour = (
            listen_time_weekday / total_listening_time
        ) * 100

        plt.figure(figsize=(16, 9))
        plt.bar(range(7), percent_occurrences_per_hour, color="skyblue")
        for index, value in enumerate(percent_occurrences_per_hour):
            formatedValue = ""
            if value > 1:
                formatedValue = f"{round(value, 2)}%"
            plt.text(
                index,
                value / 2,
                formatedValue,
                ha="center",
                va="center",
                fontsize=15,
                rotation=90,
            )

        plt.title("Percent of listening time per weekday", fontsize=20)
        plt.xlabel("Days", fontsize=15)
        plt.ylabel("Percent of total listening time", fontsize=15)

        plt.xticks(
            fontsize=15,
            ticks=[0, 1, 2, 3, 4, 5, 6],
            rotation=45,
            labels=[
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ],
        )
        plt.yticks(fontsize=15)

        plt.tight_layout()
        self.__pdf_pages.savefig()

    def writeToFile(self) -> None:
        self.__pdf_pages.close()
