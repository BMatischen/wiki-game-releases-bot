# wiki-game-releases-bot
> A Discord bot that can be used to get information on video game releases and stay informed on the latest releases.
> Data for video game releases are obtained from Wikipedia.

## Contents
* [Features](#features)
* [Main Tools](#main-tools)
* [Setup](#setup)
* [Credit](#credit)

## Features
- List monthly releases for chosen month and year
- List games released last week, today and next week
- Schedule and manage daily channel notifications for releases

## Main Tools
- Python 3.9 and discord.py for writing the bot
- Pandas for web-scraping and data processing
- Motor and MongoDB for notification storage

## Setup
1. Clone repository and setup virtual environment
2. In command line type: pip install -r requirements.txt to install packages
3. Create a MongoDB database
4. Make a new bot app via the Discord Developer Portal
5. Store yout bot token, your MongoDB database URL and database and collection names in virtual environment variables

## License
Apache License 2.0
https://github.com/BMatischen/wiki-game-releases-bot/blob/master/LICENSE

## Credit
Bot logo: By Rickterto - Own work, CC BY 4.0, https://commons.wikimedia.org/w/index.php?curid=44567898
