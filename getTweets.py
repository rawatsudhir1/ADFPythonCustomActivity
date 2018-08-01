#Below code is written and tested on Python 2.7
import tweepy
import sys
import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors


from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.common.credentials import ServicePrincipalCredentials


class IDisposable:
    """ A context manager to automatically close an object with a close method
    in a with statement. """
    def __init__(self, obj):
        self.obj = obj
    def __enter__(self):
        return self.obj # bound to target
    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self = None
credentials = None

def auth_callback(server, resource, scope):
    credentials = ServicePrincipalCredentials(
        client_id = 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX', #Azure AD APP Application ID
        secret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX', #Secret
        tenant = 'XXXXXXXX-XXXX-XXXX-XXXX-XX-XXXXXXXXXXXX', #Azure AD Directory ID
        resource = "https://vault.azure.net"
    )
    token = credentials.token
    return token['token_type'], token['access_token']

def insertintoCosmosDB(cdbhost, cdbmasterkey, tweetDate, tweetText):
    tweetmessage = {'tweetDate': str(tweetDate),'id' : str(tweetDate), 'tweetText': tweetText}
    _database_link = 'dbs/tweetdb'
    _collection_link = _database_link + '/colls/tweetcollec'
    with IDisposable(document_client.DocumentClient(cdbhost, {'masterKey': cdbmasterkey} )) as client:
        try:
            client.CreateDocument(_collection_link, tweetmessage, options=False)
        except errors.DocumentDBError as e:
            if e.status_code == 409:
                pass
            else:
                raise errors.HTTPFailure(e.status_code)

def main():
 # Twitter application key
    client = KeyVaultClient(KeyVaultAuthentication(auth_callback))
    _appkey = client.get_secret("https://XXXX.vault.azure.net/", "Twitter-appkey", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version
    _appsecret= client.get_secret("https://XXXXXX.vault.azure.net/", "Twitter-appsecret", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version
    _appaccesstoken = client.get_secret("https://XXXXXXXXXXX.vault.azure.net/", "Twitter-appaccesstoken", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version
    _appaccesstokensecret = client.get_secret("https://XXXXXXXXXXX.vault.azure.net/", "Twitter-appaccesstokensecret", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version

    _tweetTag= sys.argv[1] # like Azure 
    _tweetReadSince=  sys.argv[2] #date from when you want to read tweets like '2018/07/28'
    _RandomId = sys.argv[3] #Azure Data Factory Pipeline ID 'testrun' 
  
# CosmosDB Credential
    _cdbhost = client.get_secret("https://XXXXXXXXX.vault.azure.net/", "cosmosdbURI", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version
    _cdbmasterkey = client.get_secret("https://XXXXXXXX.vault.azure.net/", "cosmosdbPK", "XXXXXXXXXXXXXXXXXXXXXXXXXX") # KeyVault URL, Secret, Version
    
#hashtag, tweetreadsince, filename includes pipeline id, 
    auth = tweepy.OAuthHandler(_appkey.value, _appsecret.value)
    auth.set_access_token(_appaccesstoken.value, _appaccesstokensecret.value)
    tweetapi = tweepy.API(auth,wait_on_rate_limit=True)

    for tweet in tweepy.Cursor(tweetapi.search,q=_tweetTag,lang="en", since=_tweetReadSince).items(15):
        try:
            if tweet.text.encode('utf-8') != '' : 
                insertintoCosmosDB (_cdbhost.value, _cdbmasterkey.value, tweet.created_at,tweet.text.encode('utf-8'))
        except errors.DocumentDBError as e:
            if e.status_code == 409:
                pass
            else:
                raise errors.HTTPFailure(e.status_code)
                print("Error while fetching and storing tweets!!!")
            break
    
if __name__ == "__main__":
	main()
