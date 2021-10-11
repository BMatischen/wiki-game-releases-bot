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

c_error = discord.Colour.red()
c_info = discord.Colour.blue()


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
                           description=f"{ctx.message.author} is missing permission to Manage Messages",
                           color=c_error)
        await ctx.send(embed=em)

    # Send error message if bot does not have permission to send embeds
    elif isinstance(error, commands.BotMissingPermissions):
        msg = "Error!\nBot does not have permission to send embeds"
        await ctx.send(msg)
        print(error.missing_perms)

    # Send error message if command invoked without required argument
    elif isinstance(error, commands.MissingRequiredArgument):
        em = discord.Embed(title="Error",
                           description=f"Command is missing the required argument {error.param}",
                           color=c_error)
        await ctx.send(embed=em)



""" Display embedded list of commands with their names and descriptions """


@client.command(brief="""Enter command name after invoking for detailed description
                      of the command""",
                help="""Enter command name after invoking for detailed description
                      of the command""")
@commands.bot_has_permissions(embed_links=True)
async def help(ctx, command_name=None):
    em = discord.Embed(title="Help",
                       color=c_info)
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
        em.color = c_error
            
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
                              color=c_info)
        await ctx.send(embed=embed)

    except Exception as e:
        print(traceback.format_exc())
        msg = "Unable to get required data!"
        title = "Error"
        em = discord.Embed(title=title,
                           description=msg,
                           color=c_error)
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
                           color=c_info)
        await ctx.send(embed=em)

    except Exception as e:
        print(traceback.format_exc())
        msg = "Unable to get required data!"
        title = "Error"
        em = discord.Embed(title=title,
                           description=msg,
                           color=c_error)
        await ctx.send(embed=em)


""" List games to be released in next 7 days in an embed."""


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
                           color=c_info)
        await ctx.send(embed=em)

    except Exception as e:
        print(traceback.format_exc())
        msg = "Unable to get required data!"
        title = "Error"
        em = discord.Embed(title=title,
                           description=msg,
                           color=c_error)
        await ctx.send(embed=em)



""" Record new channel notification subscription.
    If channel already subscribed display current subscription info.
    Optional 4-digit 24h clock_time is used to get new time of day
    to set posting of notifications.
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
    data = await db_table.find_one({'_id': channel.id})

    if data is not None:
        # Show current notification date for channel
        notify_date = data['notify_date']
        msg = f"""To change notification time, type {prefix}set
                  To stop notifications in this channel, type {prefix}stop"""
        title = "Found Existing Subscription"
        em = discord.Embed(title=title,
                       description=msg,
                       color=c_info)
        em.add_field(name="Next Notification Due",
                     value=notify_date.strftime("%d %B %Y %H:%M (UTC+0)"),
                     inline=False)
        await ctx.send(embed=em)

    else:
        if clock_time is not None:
            try:
                # Check input is 4 characters long and try to convert to 24h clock time
                assert(len(clock_time) == 4)
                time_set = time.strptime(clock_time, "%H%M")
                notify_date = datetime.datetime(next_date.year, next_date.month,
                                                next_date.day, time_set.tm_hour,
                                                time_set.tm_min, 0, 0)
            except (ValueError, AssertionError) as e:
                print(traceback.format_exc())
                # Send error message embed if input is invalid
                msg = """Invalid time of day given!
                         It must be a valid 4-digit 24h clock time between 0000-2359."""
                title = "Error"
                em = discord.Embed(title=title,
                                   description=msg,
                                   color=c_error)
                return await ctx.send(embed=em)
        else:
            notify_date = next_date

        # Add new document for channel and display success message
        new_doc = {'_id': channel.id, 'notify_date': notify_date}
        result = await db_table.insert_one(new_doc)

        msg = f"""{ctx.message.author} has enabled daily notifications about releases in {channel}.\n
                  To change notification time, type {prefix}set
                  To stop notifications in this channel, type {prefix}stop"""
        title = "Channel Subscribed"
        em = discord.Embed(title=title,
                       description=msg,
                       color=c_info)
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
    colour = c_info
    data = await db_table.find_one({'_id': channel.id})

    # If channel in database, remove and post confirmation message
    if data is not None:
        result = await db_table.delete_one(data)
        msg = f"Daily notifications for {channel} disabled by {ctx.message.author}"
        title = "Channel Unsubscribed"

    # If channel not found set error message
    else:
        msg = f"""Channel does not receive notifications!
                  To enable notifications in this channel type {prefix}notify"""
        title = "Error"
        colour = c_error

    em = discord.Embed(title=title,
                       description=msg,
                       color=colour)
    await ctx.send(embed=em)



""" Update channel notification time when command is invoked.
    Requires 24-hour clock time string as input """


@client.command(name='set',
                help="""Set a new time for notifications in the current channel.
                        The date when notifications are posted will not change.\n
                        The <clock_time> required argument is a 4 digit number which gives the time of day in 24h clock format.
                        e.g. to set notifications for 6:30 PM, !notify 1830.
                        e.g. to set notifications for 4:15 AM, !notify 0415\n
                        Notification dates are given in UTC+0 timezone format.\n
                        Note: Users must have permission to Manage Messages to use notification system.""",
                brief="Set new time for channel daily notifications")
@commands.bot_has_permissions(embed_links=True)
@commands.has_permissions(manage_messages=True)
async def set_notify_time(ctx, clock_time):
    channel = ctx.message.channel
    try:
        data = await db_table.find_one({'_id': channel.id})
        if data is not None:
            # Check input is 4 characters long and convert to 24h clock format
            # Then use it to get new notification date
            assert(clock_time is not None)
            assert(len(clock_time) == 4)
            time_set = time.strptime(clock_time, "%H%M")
            old_date = data['notify_date']
            notify_date = datetime.datetime(old_date.year, old_date.month,
                                            old_date.day, time_set.tm_hour,
                                            time_set.tm_min, 0, 0)

            # Find document for channel and update notification date
            find_query = {'_id': data['_id']}
            update_query = {'$set': {'notify_date': notify_date}}
            result = await db_table.update_one(find_query, update_query)

            # Send success message with new date and time
            msg = f"{ctx.message.author} set a new notification time for {channel}"
            em = discord.Embed(title="New Time Set",
                               description=msg,
                               color=c_info)
            em.add_field(name="New Notification Time",
                         value=notify_date.strftime("%d %B %Y %H:%M (UTC+0)"),
                         inline=False)
            await ctx.send(embed=em)

        else:
            # Post error message if no document for channel found
            msg = f"""Channel does not receive notifications!
                      To start notifications type {prefix}notify"""
            em = discord.Embed(title="Error",
                               description=msg,
                               color=c_error)
            await ctx.send(embed=em)
            
            
    except (ValueError, AssertionError) as e:
        # Post error message for if invalid clock time given
        print(traceback.format_exc())
        msg = """Invalid time of day given!
                 It must be a valid 4-digit 24h clock time between 0000-2359."""
        title = "Error"
        em = discord.Embed(title=title,
                           description=msg,
                           color=c_error)
        await ctx.send(embed=em)
    



""" Scrape release data from Wikipedia for current date and
    post notification to any subscribed channels."""


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

            # Set new future notification date for each channel
            for row in await data.to_list(length=None):
                old_dt = row['notify_date']
                notify_date = ((curr_date + datetime.timedelta(hours=24))
                               .replace(hour=old_dt.hour,
                                        minute=old_dt.minute,
                                        second=old_dt.second,
                                        microsecond=0)
                               )
                find_query = {'_id': row['_id']}
                update_query = {'$set': {'notify_date': notify_date}}
                result = await db_table.update_one(find_query, update_query)

                # Create embed with the releases list and post to
                # subscribed channel
                em = discord.Embed(title="Today's Releases",
                                   description=msg,
                                   color=c_info)
                em.add_field(
                    name="Next Notification Due",
                    value=notify_date.strftime("%d %B %Y %H:%M (UTC+0)"),
                    inline=False)
                channel = client.get_channel(row['_id'])
                await channel.send(embed=em)
                
    except Exception as e:
        print(traceback.format_exc())




client.run(os.getenv('RELEASES_TOKEN'))
