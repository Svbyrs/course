import pandas as pd
import asyncio
import discord
from great_expectations.dataset import PandasDataset


CSV_FILES = [
    "weather_data.csv",
    "weather_archive.csv"
]

DISCORD_TOKEN = "TOKEN"
CHANNEL_ID = ID
REPORT_FILE = "ge_report.txt"



def generate_report():
    all_reports = []

    for csv_file in CSV_FILES:
        df = pd.read_csv(csv_file)
        ge_df = PandasDataset(df)

        results = [
            ("temperature not null", ge_df.expect_column_values_to_not_be_null("temperature").success),
            ("temperature range", ge_df.expect_column_values_to_be_between("temperature", -50, 50).success),
            ("windspeed not null", ge_df.expect_column_values_to_not_be_null("windspeed").success),
        ]

        report = "\n".join(
            f"{name}: {'OK' if success else 'FAILED'}"
            for name, success in results
        )

        section = (
            f"\n===== DATA QUALITY REPORT: {csv_file} =====\n"
            f"{report}\n"
        )

        print(section)
        all_reports.append(section)

    final_report = "\n".join(all_reports)

    with open(REPORT_FILE, "w") as f:
        f.write(final_report)

    return final_report


async def send_to_discord(report):
    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f"Logged in as {client.user}")
        channel = client.get_channel(CHANNEL_ID)

        if channel is None:
            print("ERROR: Channel not found")
            await client.close()
            return

        await channel.send(
            f"Weather Data Quality Report\n```\n{report}\n```"
        )

        print("Report sent to Discord")
        await client.close()

    await client.start(DISCORD_TOKEN)



if __name__ == "__main__":
    report = generate_report()
    asyncio.run(send_to_discord(report))
