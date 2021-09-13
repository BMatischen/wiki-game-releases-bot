import discord
from discord.ext import commands, tasks
import os
import numpy as np
import pandas as pd
import datetime
import time
import traceback
import motor.motor_asyncio


prefix = '!'
client = commands.Bot(command_prefix=prefix, help_command=None)
table_url = "http://en.wikipedia.org/wiki/{0}_in_video_games"
cluster = motor.motor_asyncio.AsyncIOMotorClient(os.getenv('CLUSTER'))
db_table = cluster[os.getenv('DATABASE')][os.getenv('TABLE')]


""" Reads wikipedia tables from article for chosen year and filters for
tables for releases"""


def get_year_data(year):
    df = None
    url = None
    try:
        url = table_url.format(str(year))
        tables = pd.read_html(url, match='Title')
        months = []
        headings = ['Month', 'Day', 'Title']

        # Keep tables with first 3 columns of release tables as expected
        for frame in tables:
            curr_headings = list(frame.columns[:3])
            if curr_headings == headings:
                months.append(frame)

        # Merge kept tables. Replace TBA dates with NaN to remove later
        df = (pd.concat(months)[['Month', 'Day', 'Title']]
              .replace('TBA', np.NaN))

        # Remove rows with inappropriate month data
        months = ["January", "February", "March",
                  "April", "May", "June",
                  "July", "August", "September",
                  "October", "November", "December"]
        df['Month'] = df['Month'].apply(lambda x: x.title())
        df = df[df['Month'].isin(months)].dropna()

        df['Title'] = df['Title'].replace(r"\[.*\]", "", regex=True)

        # Add and clean columns for date data
        # Then make string column for dates and convert column to datetime
        df['Year'] = pd.Series([year] * len(df['Month']))
        df['Day'] = df['Day'].astype(int)
        df['Date'] = df[['Year', 'Month', 'Day']].apply(
            lambda x: '-'.join(x.values.astype(str)), axis='columns')
        df['Date'] = pd.to_datetime(df['Date'])

        df.sort_values(by='Date', inplace=True)
        df.drop(['Year', 'Day', 'Month'], axis='columns', inplace=True)

    except Exception as e:
        raise e
    return df, url


@client.event
async def on_ready():
    print("Ready as {0.user}".format(client))
    check_notifications.start()



""" Check for errors raised after invoking commands """


@client.event
async def on_command_error(ctx, error):
    # Send embedded error message if user does not have appropriate permissions
    if isinstance(error, commands.MissingPermissions):
        em = discord.Embed(title="Error",
                           description=f"{ctx.message.author} is missing permissions to run this command",
                           color=discord.Colour.red())
        await ctx.send(embed=em)

    # Send error message if bot does not have permission to send embeds
    elif isinstance(error, commands.BotMissingPermissions):
        msg = "Error!\nBot does not have permissions to send embeds"
        await ctx.send(msg)
        print(error.missing_perms)



""" Display embedded list of commands with their names and descriptions """


@client.command(brief="""Enter command name after invoking for detailed description
                      of the command""",
                help="""Enter command name after invoking for detailed description
                      of the command""")
@commands.bot_has_permissions(embed_links=True)
async def help(ctx, command_name=None):
    em = discord.Embed(title="Help",
                       color=discord.Colour.blue())
    commands = {c.name: c for c in client.commands}
    
    if command_name is None:
        # Display command names and brief description as default
        for c in commands.values():
            em.add_field(name=f"{c.name} {c.signature}",
                         value=c.brief,
                         inline=False)
    elif command_name in commands.keys():
        # Display detailed description of chosen command
        comm = commands[command_name]
        em.add_field(name=f"{comm.name} {comm.signature}",
                     value=comm.help,
                     inline=False)
    else:
        # Display error message for invalid commands
        em.add_field(name="Error",
                     value=f"The command {command_name} does not exist!",
                     inline=False)
        em.color = discord.Colour.red()
            
    await ctx.send(embed=em)
        


""" List video game releases for chosen month and year,
sorted by date, in embed"""


@client.command(name='list', help="""
                    List video game releases for given month and year\n
                     - Get releases for current month: !list
                     - Get releases for a different month of year: !list [month]
                       (e.g. !list january)
                     - Get releases for different month and year: !list [month] [year]
                       (e.g. !list august 2020)\n
                     NB: Can only go back upto 2015 currently""",
                brief="List video game releases for given month and year")
@commands.bot_has_permissions(embed_links=True)
async def list_releases(ctx, month=None, year=None):
    curr_date = ctx.message.created_at

    # Use date that message sent as default arguments, else get user input
    if month is None:
        curr_month = curr_date.strftime('%B')
    else:
        curr_month = month.title()
    if year is None:
        curr_year = curr_date.year
    else:
        curr_year = year

    try:
        df, wiki_url = get_year_data(curr_year)
        df = df[df['Date'].dt.strftime('%B') == curr_month]
        df['Day'] = df['Date'].dt.strftime('%d')

        # Construct list of games ordered by date
        games = tuple(zip(df['Day'], df['Title']))
        msg = f"{len(games)} games released this month\n\n"
        for day, title in games:
            msg += f"{day}  {title}\n"

        embed = discord.Embed(title=f"Releases for {curr_month} {curr_year}",
                              url=wiki_url, description=msg,
                              color=0xFF5733)
        await ctx.send(embed=embed)

    except Exception as e:
        print(traceback.format_exc())
        msg = "Unable to get required data!"
        title = "Error"
        em = discord.Embed(title=title,
                           description=msg,
                           color=discord.Colour.red())
        await ctx.send(embed=em)


""" List games released in last 7 days and on current day in an embed."""


@client.command(name='new',
                help="Show games released in past 7 days and today",
                brief="Show games released in past 7 days and today")
@commands.bot_has_permissions(embed_links=True)
async def post_new(ctx):
    curr_date = ctx.message.created_at.replace(hour=0, minute=0,
                                               second=0, microsecond=0)
    last_date = curr_date - datetime.timedelta(days=7)
    try:
        df = get_year_data(curr_date.year)[0]
        # If there is change in years during week, get data from other year
        if last_date.year != curr_date.year:
            df_last = get_year_data(last_date.year)[0]
            df = pd.concat([df, df_last])

        # Get releases in last 7 days and for current day
        last_week = df[df['Date'].between(last_date,
                                          curr_date,
                                          inclusive='left')]

        mask = ((df['Date'].dt.day == curr_date.day)
                & (df['Date'].dt.month == curr_date.month)
                & (df['Date'].dt.year == curr_date.year)
                )
        today = df.where(mask).dropna()

        # Construct lists of games ordered by date
        games = tuple(zip(last_week['Date'], last_week['Title']))
        msg = f"Last 7 days: {len(games)} games released\n\n"
        for date, title in games:
            row = f"{date.strftime('%d')} {date.strftime('%b')}:  {title}\n"
            msg += row

        games = tuple(zip(today['Date'], today['Title']))
        msg += f"\nToday: {len(games)} games released\n\n"
        for date, title in games:
            msg += f"{title}\n"

        em = discord.Embed(title="Newest Releases",
                           description=msg,
                           color=0xFF5733)
        await ctx.send(embed=em)

    except Exception as e:
        print(traceback.format_exc())
        msg = "Unable to get required data!"
        title = "Error"
        em = discord.Embed(title=title,
                           description=msg,
                           color=discord.Colour.red())
        await ctx.send(embed=em)


""" List games to be released in next 7 days in an embed.
    Requires users who invoke this command to have permission
    to manage messages. """


@client.command(name='soon', help="Show games releasing in the next 7 days",
                brief="Show games releasing in the next 7 days")
@commands.bot_has_permissions(embed_links=True)
async def post_upcoming(ctx):
    curr_date = ctx.message.created_at.replace(hour=0, minute=0,
                                               second=0, microsecond=0)
    end_date = curr_date + datetime.timedelta(days=7)

    try:
        df = get_year_data(curr_date.year)[0]
        # If there is change in years during week, get data from other year
        if end_date.year != curr_date.year:
            df_last = get_year_data(end_date.year)[0]
            df = pd.concat([df, df_last])

        upcoming = df[df['Date'].between(curr_date,
                                         end_date,
                                         inclusive='right')]

        # Construct list of games ordered by date
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
        print(traceback.format_exc())
        msg = "Unable to get required data!"
        title = "Error"
        em = discord.Embed(title=title,
                           description=msg,
                           color=discord.Colour.red())
        await ctx.send(embed=em)


""" Record new channel notification subscription. If 4-digit number given,
    try to convert it to 24h clock time and set notification time to it.
    Otherwise use time of command invocation.
    Requires users to have permission to manage messages. """


@client.command(name='notify',
                help="""Schedule daily notifications about releases in the current channel.\n
                        - Set notifications to be posted at current time: !notify
                        - Set notifications to be posted at certain time of day: !notify [clock_time]\n
                        [clock_time] is a 4 digit number which gives the time of day in 24h clock format.
                        e.g. to set notifications for 6:30 PM, !notify 1830.
                        e.g. to set notifications for 4:15 AM, !notify 0415\n
                        Notification dates are given in UTC+0 timezone format.\n
                        Note: Users must have permission to Manage Messages to use notification system.""",
                brief="Enable daily notifications about releases in the current channel")
@commands.bot_has_permissions(embed_links=True)
@commands.has_permissions(manage_messages=True)
async def notify(ctx, clock_time=None):
    channel = ctx.message.channel
    curr_date = ctx.message.created_at
    next_date = curr_date + datetime.timedelta(hours=24)

    if clock_time is not None:
        try:
            # Check input is 4 characters long and try to convert it
            assert(len(clock_time) == 4)
            time_set = time.strptime(clock_time, "%H%M")
            notify_date = datetime.datetime(next_date.year, next_date.month,
                                            next_date.day, time_set.tm_hour,
                                            time_set.tm_min, 0, 0)
        except (ValueError, AssertionError) as e:
            print(traceback.format_exc())
            msg = """Invalid time of day given!
                     It must be a 4-digit number between 0000-2359."""
            title = "Error"
            em = discord.Embed(title=title,
                               description=msg,
                               color=discord.Colour.red())
            await ctx.send(embed=em)
            return
    else:
        notify_date = next_date

    msg = ""
    title = ""
    data = await db_table.find_one({'_id': channel.id})

    # If channel not in database, store notifcation data and set appropriate
    # message
    if data is None:
        new_ch = {'_id': channel.id, 'notify_date': notify_date}
        result = await db_table.insert_one(new_ch)

        msg = f"""{ctx.message.author} has enabled daily notifications about releases in {channel}.
                To disable notifications in {channel}, use command {prefix}stop"""
        title = "Channel Subscribed"
        colour = 0xFF5733

    # If channel found, embed will have error message with next notification date
    else:
        notify_date = data['notify_date']
        msg = "Channel already receives notfications!"
        title = "Error"
        colour = discord.Colour.red()

    em = discord.Embed(title=title,
                       description=msg,
                       color=colour)
    em.add_field(name="Next Notification Due",
                 value=notify_date.strftime("%d %B %Y %H:%M (UTC+0)"),
                 inline=False)
    await ctx.send(embed=em)




""" Delete channel subscription from database. If channel wasn't subscribed,
    post embed with error message"""


@client.command(name='stop',
                help="""Unsubscribe from daily notifications in the current channel\n
                        Note: Users must have permission to Manage Messages to use notification system.""",
                brief="""Unsubscribe from daily notifications in the current channel.""")
@commands.bot_has_permissions(embed_links=True)
@commands.has_permissions(manage_messages=True)
async def remove_from_notify(ctx):
    channel = ctx.message.channel
    msg = ""
    title = ""
    colour = 0xFF5733
    data = await db_table.find_one({'_id': channel.id})

    # If channel in database, remove and set confirmation message
    if data is not None:
        result = await db_table.delete_one(data)
        msg = f"Daily notifications for {channel} disabled by {ctx.message.author}"
        title = "Channel Unsubscribed"

    # If channel not found set error message
    else:
        msg = "Channel does not receive notifications"
        title = "Error"
        colour = discord.Colour.red()

    em = discord.Embed(title=title,
                       description=msg,
                       color=colour)
    await ctx.send(embed=em)



""" Scrape release data from Wikipedia for current date and
    post notification to subscribed channels."""


@tasks.loop(minutes=5)
@commands.bot_has_permissions(embed_links=True)
async def check_notifications():
    try:
        curr_date = datetime.datetime.now()
        data = db_table.find({'notify_date': {'$lte': curr_date}})
        if data is not None:
            # Get releases for current date
            df = get_year_data(curr_date.year)[0]
            mask = ((df['Date'].dt.day == curr_date.day)
                    & (df['Date'].dt.month == curr_date.month)
                    & (df['Date'].dt.year == curr_date.year)
                    )
            today = df.where(mask).dropna()

            # Construct list of releases sorted by date
            games = tuple(zip(today['Date'], today['Title']))
            msg = f"{len(games)} games released\n\n"
            for date, title in games:
                msg += f"{title}\n"
            
            for row in await data.to_list(length=None):
                # Set new future notification date for channel and
                # update database table with new date
                notify_date = curr_date + datetime.timedelta(hours=24)
                find_query = {'_id': row['_id']}
                update_query = {'$set': {'notify_date': notify_date}}
                result = await db_table.update_one(find_query, update_query)

                # Create embed with the releases list and post to
                # subscribed channel
                em = discord.Embed(title="Today's Releases",
                                   description=msg,
                                   color=0xFF5733)
                em.add_field(
                    name="Next Notification Due",
                    value=notify_date.strftime("%d %B %Y %H:%M (UTC+0)"),
                    inline=False)
                channel = client.get_channel(row['_id'])
                await channel.send(embed=em)
                
    except Exception as e:
        print(traceback.format_exc())




client.run(os.getenv('RELEASES_TOKEN'))
