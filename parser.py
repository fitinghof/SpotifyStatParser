import pandas as pd
import matplotlib.pyplot as plt
import japanize_matplotlib
import re
from pathlib import Path
from datetime import date, timedelta
from matplotlib.backends.backend_pdf import PdfPages


class WrappedMaker:
    # -------------
    _df: pd.DataFrame
    _pdf_pages: PdfPages

    _top_songs: pd.DataFrame = None
    _top_artists: pd.DataFrame = None

    _extended: bool = False

    def __init__(
        self,
        start_date: date = date(1, 1, 1),
        end_date: date = date(4000, 12, 31),
        pdf_target_path: Path = Path("."),
        history_src_dir: Path = Path("./StreamingHistory"),
    ) -> pd.DataFrame:
        self._df = pd.DataFrame()
        directory = Path(history_src_dir)
        streaming_history_simple = re.compile(r"StreamingHistory_music_\d+\.json")
        streaming_history_extended = re.compile(
            r"Streaming_History_Audio_\d{4}(?:-\d{4})?_\d+\.json"
        )
        simple: bool = False
        for file_path in directory.iterdir():
            if file_path.is_file():
                if streaming_history_simple.match(file_path.name):
                    self._df = pd.concat([self._df, pd.read_json(file_path)])
                    simple = True
                if streaming_history_extended.match(file_path.name):
                    self._df = pd.concat([self._df, pd.read_json(file_path)])
                    self._extended = True
        if simple and self._extended:
            self._df = self._df.drop(
                [
                    "platform",
                    "master_metadata_album_album_name",
                    "spotify_track_uri",
                    "reason_start",
                    "reason_end",
                    "shuffle",
                    "skipped",
                    "platform",
                ]
            )
        if self._extended:
            self._df.rename(
                columns={
                    "master_metadata_track_name": "trackName",
                    "master_metadata_album_artist_name": "artistName",
                    "ts": "endTime",
                    "ms_played": "msPlayed",
                },
                inplace=True,
            )
            self._df = self._df.drop(
                [
                    "conn_country",
                    "ip_addr",
                    "episode_name",
                    "episode_show_name",
                    "spotify_episode_uri",
                    "audiobook_title",
                    "audiobook_uri",
                    "audiobook_chapter_uri",
                    "audiobook_chapter_title",
                    "offline",
                    "offline_timestamp",
                    "incognito_mode",
                ],
                axis=1,
            )

        self._df = self._df.drop_duplicates()

        self._df["endTime"] = pd.to_datetime(self._df["endTime"]).dt.tz_convert(None)
        self._df["msPlayed"] = pd.to_numeric(self._df["msPlayed"])

        self.__start_date = max(start_date, self._df["endTime"].min().date())
        self.__end_date = min(
            end_date + timedelta(days=1),
            self._df["endTime"].max().date() + timedelta(days=1),
        )

        self._df = self._df.query(
            f"endTime >= '{self.__start_date}' & endTime <= '{self.__end_date}'"
        )

        self._pdf_pages = PdfPages(Path.joinpath(pdf_target_path, "Wrapped.pdf"))

    # ----------------

    # Front page
    def front_page(self) -> None:
        plt.figure(figsize=(16, 9))  # Standard letter size
        plt.text(0.5, 0.95, "Spotify Wrapped", fontsize=30, ha="center", va="top")
        period = self.__end_date - self.__start_date
        info_string = f"Stats for period {self.__start_date} to {self.__end_date}\n".join(
            [
                f"Total listening time: {(self._df["msPlayed"].sum() / 3600000).round(2)}h\n",
                f"Average listening time per day: {(self._df['msPlayed'].sum() / period.days / 3600000).round(2)}h\n",
            ]
        )

        plt.text(0.5, 0.85, info_string, fontsize=20, ha="center", va="top")
        plt.axis("off")  # Hide axes

        # Save the text page to the PDF
        self._pdf_pages.savefig()
        plt.close()

    def __make_top_songs(self, nrof_songs) -> None:
        if self._top_songs is None or self._top_songs.shape[0] < nrof_songs:
            top_songs = (
                self._df.groupby(["artistName", "trackName"])["msPlayed"]
                .count()
                .sort_values(ascending=False)
                .rename("playCount")
                .head(nrof_songs)
            )
            self._top_songs = pd.DataFrame(top_songs).sort_values(
                by="playCount", ascending=True
            )

    def top_songs(self, nrof_songs: int = 10) -> None:
        self.__make_top_songs(nrof_songs)

        top_songs_df = self._top_songs.head(nrof_songs)

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

        plt.title(f"Top {nrof_songs} songs", fontsize=20)
        plt.ylabel("Song Title", fontsize=15)
        plt.xlabel("Play Count", fontsize=15)

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self._pdf_pages.savefig()
        self._top_songs = top_songs_df

    def top_songs_chart(self, nrof_songs: int = 10, rolling_window: int = 31):
        self.__make_top_songs(nrof_songs)

        top_songs_full = self._df[
            self._df.set_index(["artistName", "trackName"]).index.isin(
                self._top_songs.head(nrof_songs).index
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

        for artist, track in reversed(self._top_songs.index):
            song_data = top_songs_daily.query(
                f"artistName == '{artist}' & trackName == '{track}'"
            ).reset_index()
            song_data["rolling_playCount"] = (
                song_data["playCount"].rolling(window=rolling_window).mean()
            )
            plt.plot(
                song_data["endTime"], song_data["rolling_playCount"], label=f"{track}"
            )

        plt.legend(fontsize=15)
        plt.title(
            f"Top {nrof_songs} Songs listening time rolling {rolling_window} day average",
            fontsize=20,
        )
        plt.xlabel("Date", fontsize=15)
        plt.ylabel("Play Count", fontsize=15)
        plt.grid()

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self._pdf_pages.savefig()

    def __make_top_artist(self, nrof_artists) -> None:
        if self._top_artists is None or self._top_artists.shape[0] < nrof_artists:
            top_artists = (
                self._df.groupby(["artistName"])["msPlayed"]
                .count()
                .sort_values(ascending=False)
                .rename("playCount")
                .head(nrof_artists)
            )
            self._top_artists = pd.DataFrame(top_artists).sort_values(
                by="playCount", ascending=True
            )

    def top_artists(self, nrof_artists: int = 10) -> None:

        self.__make_top_artist(nrof_artists)

        top_artist_df = self._top_artists.head(nrof_artists)

        plt.figure(figsize=(16, 9))

        top_artist_df = top_artist_df.sort_values(by="playCount", ascending=True)

        plt.barh(top_artist_df.index, top_artist_df["playCount"], color="skyblue")
        for index, value in enumerate(top_artist_df["playCount"]):
            plt.text(
                value / 2, index, f"{value}", ha="center", va="center", fontsize=15
            )

        plt.title(f"Top {nrof_artists} artists", fontsize=20)
        plt.ylabel("Artist name", fontsize=15)
        plt.xlabel("Play Count", fontsize=15)

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self._pdf_pages.savefig()

        self._top_artists = top_artist_df

    def top_artists_chart(self, nrof_artists: int = 10, rolling_window: int = 31):
        self.__make_top_artist(nrof_artists)

        top_artists_full = self._df[
            self._df.set_index(["artistName"]).index.isin(
                self._top_artists.head(nrof_artists).index
            )
        ]

        top_artists_daily = (
            top_artists_full.groupby(["artistName"]).resample("D", on="endTime").count()
        )
        top_artists_daily["playCount"] = top_artists_daily["artistName"]
        top_artists_daily = top_artists_daily.drop(
            ["artistName", "trackName", "msPlayed"], axis=1
        )

        plt.figure(figsize=(16, 9))

        for artist in reversed(self._top_artists.index):
            song_data = top_artists_daily.query(
                f"artistName == '{artist}'"
            ).reset_index()
            song_data["rolling_playcount"] = (
                song_data["playCount"].rolling(window=rolling_window).mean()
            )
            plt.plot(
                song_data["endTime"], song_data["rolling_playcount"], label=f"{artist}"
            )

        plt.title(
            f"Top {nrof_artists} artists listening time rolling {rolling_window} day average",
            fontsize=20,
        )

        plt.legend(fontsize=15)
        plt.xlabel("Date", fontsize=15)
        plt.ylabel("Play Count", fontsize=15)
        plt.grid()

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.tight_layout()
        self._pdf_pages.savefig()

    def play_time_chart(self, rolling_window: int = 31):

        playtime = self._df.resample("D", on="endTime")["msPlayed"].sum()

        # Convert to hours
        playtime = playtime.divide(3600000)

        playtime = playtime.rolling(window=rolling_window).mean()

        plt.figure(figsize=(16, 9))
        plt.plot(playtime)

        plt.title(
            f"Total playtime rolling {rolling_window} day average",
            fontsize=20,
        )
        plt.xlabel("Date", fontsize=15)
        plt.ylabel("Play Time (h)", fontsize=15)

        plt.yticks(fontsize=15)
        plt.xticks(fontsize=15)

        plt.grid()

        plt.tight_layout()
        self._pdf_pages.savefig()

    def play_time_per_hour_in_day(self):

        ms_per_hour = self._df.copy()
        ms_per_hour["hours"] = self._df["endTime"].dt.hour
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
        self._pdf_pages.savefig()

    def play_time_per_weekday(self):
        listen_time_weekday = self._df.copy()
        listen_time_weekday["weekday"] = self._df["endTime"].dt.weekday
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
        self._pdf_pages.savefig()

    def song_skip_stats(self, nrof_songs: int = 10, least_amount_listens: int = 15):
        if not self._extended:
            print(
                "-- WARNING -- \nCan not do 'song_skip_stats' due to list not being purely extended entries"
            )
            return

        grouped = (
            self._df.groupby(["artistName", "trackName", "skipped"])
            .size()
            .unstack(fill_value=0)
        )

        grouped["total"] = grouped[True] + grouped[False]

        grouped = grouped[grouped["total"] >= least_amount_listens]

        grouped["percent_skipped"] = (grouped[True] / grouped["total"]) * 100

        mostSkipped = (
            grouped[["percent_skipped", "total"]]
            .sort_values(by=["percent_skipped", "total"], ascending=False)
            .head(nrof_songs)
        )
        mostSkipped = mostSkipped.iloc[::-1]

        plt.figure(figsize=(16, 9))
        plt.barh(
            [str(i[0]) + " - " + str(i[1]) for i in mostSkipped.index],
            mostSkipped["percent_skipped"],
            color="skyblue",
        )
        for index, value in enumerate(mostSkipped["percent_skipped"]):
            plt.text(
                x=value / 2,
                y=index,
                s=f"{round(value)}%",
                ha="center",
                va="center",
                fontsize=15,
            )

        plt.xlabel("Percent Skipped", fontsize=15)
        plt.ylabel("Song", fontsize=15, rotation=90)
        plt.xticks(fontsize=15)
        plt.yticks(fontsize=15)
        plt.title(
            f"Most skipped songs by percentage over {least_amount_listens} listens",
            fontsize=20,
        )

        plt.tight_layout()
        self._pdf_pages.savefig()

        leastSkipped = (
            grouped[["percent_skipped", "total"]]
            .sort_values(by=["percent_skipped", "total"], ascending=[True, False])
            .head(nrof_songs)
            .sort_values(by=["percent_skipped", "total"], ascending=[False, True])
        )

        plt.figure(figsize=(16, 9))
        plt.barh(
            [str(i[0]) + " - " + str(i[1]) for i in leastSkipped.index],
            leastSkipped["total"],
            color="skyblue",
        )
        max_listen = leastSkipped["total"].max()
        for index, row in enumerate(leastSkipped.iterrows()):
            value = row[1]
            if value["total"] < max_listen / 4:
                continue
            plt.text(
                x=value["total"] / 2,
                y=index,
                s=f"Listens: {int(value['total'])}, skip rate: {round(value['percent_skipped'])}%",
                ha="center",
                va="center",
                fontsize=15,
            )
        plt.xlabel("Total listens", fontsize=15)
        plt.ylabel("Song", fontsize=15, rotation=90)
        plt.xticks(fontsize=15)
        plt.yticks(fontsize=15)
        plt.title(f"Least skipped songs by percentage over {15} listens", fontsize=20)

        plt.tight_layout()
        self._pdf_pages.savefig()

    def least_skipped_top_songs(self, nrof_songs: int = 10):
        if not self._extended:
            print(
                "-- WARNING -- \nCan not do 'least_skipped_top_songs' due to list not being purely extended entries"
            )
            return
        self.__make_top_songs(nrof_songs)

        top_songs_entries = self._df[
            self._df.set_index(["artistName", "trackName"]).index.isin(
                self._top_songs.head(nrof_songs).index
            )
        ]

        grouped = (
            top_songs_entries.groupby(["artistName", "trackName", "skipped"])
            .size()
            .unstack(fill_value=0)
            .head(nrof_songs)
        )

        grouped["total"] = grouped[True] + grouped[False]

        grouped["percent_skipped"] = (grouped[True] / grouped["total"]) * 100

        grouped.sort_values(by="percent_skipped", inplace=True, ascending=False)

        plt.figure(figsize=(16, 9))
        plt.barh(
            [str(i[0]) + " - " + str(i[1]) for i in grouped.index],
            grouped["percent_skipped"],
            color="skyblue",
        )
        max_skip_percent = grouped["percent_skipped"].max()
        for index, row in enumerate(grouped.iterrows()):
            value = row[1]
            x_loc = value["percent_skipped"] / 2
            x_align = 'center'
            if value["percent_skipped"] < max_skip_percent / 4:
                x_loc = value["percent_skipped"]
                x_align = 'left'
            plt.text(
                x=x_loc,
                y=index,
                s=f"Listens: {int(value['total'])}, skip rate: {round(value['percent_skipped'])}%",
                ha=x_align,
                va="center",
                fontsize=15,
            )

        plt.xlabel("Percent skipped", fontsize=15)
        plt.ylabel("Song", fontsize=15, rotation=90)
        plt.xticks(fontsize=15)
        plt.yticks(fontsize=15)
        plt.title(
            f"Top {min(nrof_songs, self._top_songs.shape[0])} least skipped top songs skip rate",
            fontsize=20,
        )

        plt.tight_layout()
        self._pdf_pages.savefig()

    def device_listening_time(self):
        if not self._extended:
            print(
                "-- WARNING -- \nCan not do 'device_listening_time' due to list not being purely extended entries"
            )
            return
        listen_time_per_device = self._df.groupby('platform')['msPlayed'].sum()

        # Define partial string matches and their corresponding labels
        partial_strings = {
            'Windows': 'Windows',
            'Linux': 'Linux',
            r'ps5|ps4|ps3|ps2|playstation': 'PlayStation',
            'Android': 'Android',
            r'ios': 'iOS',
            r'osx': 'MacOS'
        }

        # Split the DataFrame based on partial string matches and calculate the sum of msPlayed for each group
        platforms = [(label, listen_time_per_device[listen_time_per_device.index.str.contains(partial, case=False)].sum() / 3600000) for partial, label in partial_strings.items()]

        # Calculate the total listening time for all platforms
        total_listening_time = listen_time_per_device.sum() / 3600000

        # Calculate the sum of the listed platforms
        listed_platforms_sum = sum([x[1] for x in platforms])

        # Calculate the listening time for the "Other" category
        other_listening_time = total_listening_time - listed_platforms_sum
        platforms.append(("Other", other_listening_time))

        # Sort the platforms list by the second element of each tuple in descending order
        platforms.sort(key=lambda x: x[1], reverse=True)

        plt.figure(figsize=(16, 9))
        plt.barh(
            [i[0] for i in platforms],
            [i[1] for i in platforms],
            color="skyblue",
        )
        for index, value in enumerate([i[1] for i in platforms]):
            if value < max(platforms, key=lambda x: x[1])[1] / 10:
                continue
            plt.text(
                x=value / 2,
                y=index,
                s=f"{round(value)}",
                ha='center',
                va="center",
                fontsize=15,
            )

        plt.xlabel("Listening time (hours)", fontsize=15)
        plt.ylabel("Device", fontsize=15, rotation=90)
        plt.xticks(fontsize=15)
        plt.yticks(fontsize=15)
        plt.title(
            f"Listening time per device",
            fontsize=20,
        )

        plt.tight_layout()
        self._pdf_pages.savefig()
        plt.close()

    def device_listening_chart(self, rolling_window: int = 31):
        if not self._extended:
            print(
                "-- WARNING -- \nCan not do 'device_listening_chart' due to list not being purely extended entries"
            )
            return
        # Group by platform and resample by day, summing the msPlayed for each day
        listen_time_per_device = self._df.groupby(["platform", pd.Grouper(key="endTime", freq="D")])['msPlayed'].sum().reset_index()

        # Define partial string matches and their corresponding labels
        partial_strings = {
            'Windows': 'Windows',
            'Linux': 'Linux',
            r'ps5|ps4|ps3|ps2|playstation': 'PlayStation',
            'Android': 'Android',
            r'ios': 'iOS',
            r'osx': 'macOS'
        }

        plt.figure(figsize=(16, 9))

        # Plot each device over the entire time with rolling mean
        for partial, label in partial_strings.items():
            platform_data = listen_time_per_device[listen_time_per_device['platform'].str.contains(partial, case=False)].copy()

            # Ensure the data is continuous by reindexing and filling missing values
            platform_data.set_index('endTime', inplace=True)
            platform_data = platform_data.resample('D').sum().fillna(0).reset_index()

            # Calculate the rolling mean
            platform_data['rolling_mean'] = platform_data['msPlayed'].rolling(window=rolling_window).mean()

            # Plot the data
            plt.plot(platform_data['endTime'], platform_data['rolling_mean'] / 3600000, label=label)

        plt.legend(fontsize=20)
        plt.xlabel("Date", fontsize=15)
        plt.ylabel("Listening Time (hours) per day", fontsize=15)
        plt.title(
            f"Listening time per device rolling {rolling_window} day average",
            fontsize=20,
        )
        plt.xticks(fontsize=15)
        plt.yticks(fontsize=15)
        plt.tight_layout()
        self._pdf_pages.savefig()
        plt.close()

    def write_to_file(self) -> None:
        self._pdf_pages.close()
