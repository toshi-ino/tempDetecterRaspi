# coding: UTF-8
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv(verbose=True)

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

ENDPOINT_set= os.environ.get("ENDPOINT")
CLIENT_ID_set= os.environ.get("CLIENT_ID")
PATH_TO_CERT_set = os.environ.get("PATH_TO_CERT")
PATH_TO_KEY_set = os.environ.get("PATH_TO_KEY")
PATH_TO_ROOT_set = os.environ.get("PATH_TO_ROOT")