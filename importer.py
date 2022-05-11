""" Author: Will Anderson (@will-a) """

import sys
import json
import logging
from pathlib import Path

import discord
import requests

requests.packages.urllib3.disable_warnings()

logging.basicConfig(level=logging.INFO)
ROOT = str(Path(__file__).parent)
# hello
# read configs
try:
    with open(f'{ROOT}/configs.json', 'r', encoding='utf-8') as configs_file:
        configs = json.loads(configs_file.read())
except OSError:
    logging.error("ERROR: could not read configs.json")
    sys.exit(1)

discord_token = configs.get('discord_token')
if discord_token is None:
    logging.error("ERROR: No bot token found in configs.json")
    sys.exit(1)

elastic_token = configs.get('elastic_token')
if elastic_token is None:
    logging.error("ERROR: No Elastic token found in configs.json")
    sys.exit(1)

superusers = configs.get('superusers')
if superusers is None:
    logging.warning("WARNING: No superusers specified in configs.json")

elastic_base_url = configs.get('host')
if elastic_base_url is None:
    logging.error("ERROR: No host found in configs.json")
    sys.exit(1)

elastic_index = configs.get('index')
if elastic_index is None:
    logging.error("ERROR: Elastic index not provided")
    sys.exit(1)

verbose = False
client = discord.Client()


async def post_message_to_elastic(message: discord.Message) -> requests.Response:
    """
    Post Discord message to Elastic database

    params
    ------
    message : discord.Message
        discord.Message object ingested by client.

    return
    ------
    requests.Response:
        Response from Elastic POST request.
    """
    payload = {
        '@timestamp': message.created_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'author': {
            'full_user': str(message.author),
            'name': message.author.name,
            'id': str(message.author.id)
        },
        'body': message.content,
        'channel': {
            'id': str(message.channel.id),
            'name': message.channel.name
        },
        'attachments': [{'type': attachment.content_type, 'url': attachment.url} for attachment in message.attachments]
    }
    logging.debug("Payload: %s", payload)

    resp = requests.post(
        url=f'{elastic_base_url}{elastic_index}/_doc',
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json',
                 'Authorization': f"ApiKey {elastic_token}"},
        verify=False
    )

    logging.debug("Response code %d: %s", resp.status_code, resp.text)
    if verbose:
        await message.channel.send(f"Payload: `{payload}`\n{resp.status_code}: `{resp.content}`")


@client.event
async def on_ready():
    """  
    Function automatically called when Discord client is ready.
    """
    logging.info("Logged in as %s", client.user)


@client.event
async def on_message(message: discord.Message) -> None:
    """
    Function automatically called when Discord client detects a message has been sent in a chat it can see.

    params
    ------
    message : discord.Message
        Discord message object of message sent in server.
    """
    global verbose
    if message.author == client.user:
        logging.debug("Message sent by bot user, ignoring...")
        return

    if message.content is not None and message.content.startswith('!') and \
            len(message.content) > 1 and message.author.id in superusers:
        command = message.content[1:].split(' ')[0]
        logging.info("Processing command %s", command)

        if command == 'ingest':
            logging.info("Ingesting messages from %s", message.channel.name)
            await message.channel.send(f"Ingesting messages from {message.channel.name}...")
            message_count = 0
            async for historic_message in message.channel.history(limit=None):
                if historic_message.author != client.user:
                    await post_message_to_elastic(historic_message)
                    message_count += 1

            logging.info("Successfully ingested %d messages from %s.",
                         message_count, message.channel.name)
            await message.channel.send(f"Successfully ingested {message_count} messages from {message.channel.name}.")
            return
        if command == 'verbose':
            verbose = not verbose
            logging.info("Verbose toggled to %s.", verbose)
            await message.channel.send(f"Verbose toggled to {verbose}.")
            return

    await post_message_to_elastic(message)


def main():
    """ Main method """
    client.run(discord_token)


if __name__ == '__main__':
    main()
