import discord
from discord.ext import commands
import os
import numpy as np
import pandas as pd
import datetime


prefix = '!'
client = commands.Bot(command_prefix = prefix)
table_url = "http://en.wikipedia.org/wiki/{0}_in_video_games"


""" Reads wikipedia tables from article for chosen year and filters for
tables for releases"""

def get_year_data(year):
    df = None
    url = None
    try:
        url = table_url.format(str(year))
        tables = pd.read_html(url, match='Title')
        months = []
        headings = ['Month','Day','Title']

        # Keep tables with first 3 columns of release tables as expected
        for frame in tables:
            curr_headings = list(frame.columns[:3])
            if curr_headings == headings:
                months.append(frame)

        # Merge kept tables then remove 'TBA' release and misisng values
        df = (pd.concat(months)[['Month','Day','Title']]
              .replace('TBA',np.NaN)
              .dropna()
              )
        
        df['Title'] = df['Title'].replace(r"\[.*\]", "", regex=True)
        df['Year'] = pd.Series([year] * len(df['Month']))
        df['Day'] = df['Day'].astype(int)

        # Make new string column for dates by joining release data
        # Then convert to datetime64 and clean table
        df['Date'] = df[['Year','Month','Day']].apply(lambda x: '-'.join(x.values.astype(str)), axis='columns')
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(by='Date', inplace=True)
        df.drop(['Year','Day','Month'], axis='columns', inplace=True)

    except Exception as e:
        raise e
    return df, url


@client.event
async def on_ready():
    print("Ready as {0.user}".format(client))


""" List video game releases for chosen month and year,
sorted by date, in embed"""

@client.command(name='list', help="""
                    List video game releases for given month and year\n
                     - Get releases for current month: !list
                     - Get releases for a different month of year: !list [month]
                       (e.g. !list january)
                     - Get releases for different month and year: !list [month] [year]
                       (e.g. !list august 2020)\n
                     NB: Can only go back upto 2015 currently""")
async def list_releases(ctx, month=None, year=None):
    # Use date that message sent as default arguments, else get user input
    curr_date = ctx.message.created_at
    if month == None:
        curr_month = curr_date.strftime('%B')
    else:
        curr_month = month.title()
    if year == None:
        curr_year = curr_date.year
    else:
        curr_year = year

    try:
        df, wiki_url = get_year_data(curr_year)
        df = df[df['Date'].dt.strftime('%B') == curr_month]
        df['Day'] = df['Date'].dt.strftime('%d')

        games = tuple(zip(df['Day'], df['Title']))
        msg = f"{len(games)} games released this month\n\n"
        for day, title in games:
            row = f"{day}  {title}\n"
            msg += row

        embed = discord.Embed(title=f"Releases for {curr_month} {curr_year}",
                              url=wiki_url, description=msg,
                              color=0xFF5733)
        await ctx.send(embed=embed)
        
    except Exception as e:
        print(e)
        await ctx.send("Error: Unable to get data!")


""" List games released in last 7 days and on current day in an embed """

@client.command(name='new', help='Show games released in past 7 days and today')
async def post_new(ctx):
    curr_date = ctx.message.created_at
    last_date = curr_date - datetime.timedelta(days=7)
    try:
        df = get_year_data(curr_date.year)[0]
        if last_date.year != curr_date.year:
            df_last = get_year_data(last_date.year)[0]
            df = pd.concat([df, df_last])

        last_week = df[df['Date'].between(last_date,
                                          curr_date,
                                          inclusive='left')]
        today = df[df['Date'].between(curr_date, curr_date)]

        games = tuple(zip(last_week['Date'], last_week['Title']))
        msg = f"Last 7 days: {len(games)} games released\n\n"
        for date, title in games:
            row = f"{date.strftime('%d')} {date.strftime('%b')}:  {title}\n"
            msg += row

        games = tuple(zip(today['Date'], today['Title']))
        msg += f"\nToday: {len(games)} games released\n\n"
        for date, title in games:
            row = f"{title}\n"
            msg += row

        em = discord.Embed(title="Newest Releases",
                           description=msg,
                           color=0xFF5733)
        await ctx.send(embed=em)

    except Exception as e:
        print(e)
        await ctx.send("Error: Unable to get data!")


""" List games to be released in next 7 days in an embed """

@client.command(name='soon', help="Show games releasing in the next 7 days")
async def post_upcoming(ctx):
    curr_date = ctx.message.created_at
    end_date = curr_date + datetime.timedelta(days=7)
    try:
        df = get_year_data(curr_date.year)[0]
        if end_date.year != curr_date.year:
            df_last = get_year_data(end_date.year)[0]
            df = pd.concat([df, df_last])

        upcoming = df[df['Date'].between(curr_date,
                                         end_date,
                                         inclusive='right')]

        games = tuple(zip(upcoming['Date'], upcoming['Title']))
        msg = f"Next 7 days: {len(games)} to be released\n\n"
        for date, title in games:
            row = f"{date.strftime('%d')} {date.strftime('%b')}:  {title}\n"
            msg += row

        em = discord.Embed(title="Upcoming Releases",
                           description=msg,
                           color=0xFF5733)
        await ctx.send(embed=em)

    except Exception as e:
        print(e)
        await ctx.send("Error: Unable to get data!")







client.run(os.getenv('RELEASES_TOKEN'))

