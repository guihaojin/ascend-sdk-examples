# sample code to call Ascend HTTP endpoint directly

import requests

ACCESS_KEY = '<ACCESS_KEY>'
SECRET_KEY = '<SECRET_KEY>'
HOSTNAME = '<example.ascend.io>' 

# AWS Version 4 signing, copied from ascend/auth/auth.py

from datetime import datetime
from requests.auth import AuthBase
from urllib.parse import quote, urlparse

import hashlib
import hmac

SIGNED_REGION = ""
SIGNED_SERVICE = "ascend"

ISO8601_FORMAT = "%Y%m%dT%H%M%SZ"
DATE_FORMAT = "%Y%m%d"

HASHING_ALGORITHM = "AWS4-HMAC-SHA256"


class AwsV4Auth(AuthBase):
  def __init__(self, access_key, secret_key, environment_hostname, http_method):
    self.access_key = access_key
    self.secret_key = secret_key
    self.environment_hostname = environment_hostname
    self.http_method = http_method

  def __call__(self, req):
    return self.add_request_auth_headers(req)

  def _create_signature_key(self, signing_key, timestamp, region, service):
    signature = self._sign(('AWS4' + signing_key).encode('utf-8'), timestamp)
    signature = self._sign(signature, region)
    signature = self._sign(signature, service)
    signature = self._sign(signature, 'aws4_request')
    return signature

  def _sign(self, signing_key, msg):
    return hmac.new(signing_key, msg.encode('utf-8'), hashlib.sha256).digest()

  def add_request_auth_headers(self, req):
    curr_timestamp = datetime.utcnow()
    amz_timestamp = curr_timestamp.strftime(ISO8601_FORMAT)  # Needed for x-amz-date header
    date_timestamp = curr_timestamp.strftime(DATE_FORMAT)  # Needed for Authorization header

    # Step 1: Create a Canonical Request
    parsed_url = urlparse(req.url)
    canonical_path = quote(parsed_url.path if parsed_url.path else "/", safe="/-_.~")

    canonical_query = ""
    quoted_query_params = quote(parsed_url.query, safe="/-_.~")
    for query_param in sorted(quoted_query_params.split("&")):
      param_split = query_param.split("=", 1)

      k, v = param_split[0], ""
      if len(param_split) > 1:
        v = param_split[1]

      if k:
        if canonical_query:
          canonical_query += "&"
        canonical_query += u"=".join([k, v])

    canonical_headers = "host:{}\n".format(self.environment_hostname)
    canonical_headers += "x-amz-date:{}\n".format(amz_timestamp)

    signed_headers = "host;x-amz-date"

    body_hash = hashlib.sha256("".encode("utf-8")).hexdigest()

    canonical_request = "{}\n{}\n{}\n{}\n{}\n{}".format(self.http_method, canonical_path, canonical_query, canonical_headers, signed_headers, body_hash)

    # Step 2: Create a String to Sign
    scope = "{}/{}/{}/aws4_request".format(date_timestamp, SIGNED_REGION, SIGNED_SERVICE)
    hashed_canonical_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    string_to_sign = "{}\n{}\n{}\n{}".format(HASHING_ALGORITHM, amz_timestamp, scope, hashed_canonical_request)

    # Step 3: Calculate the Signature
    signing_key = self._create_signature_key(self.secret_key, date_timestamp, SIGNED_REGION, SIGNED_SERVICE)
    signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    # Step 4: Add the Signature to the HTTP Request
    authz_header = "{} Credential={}/{}, SignedHeaders={}, Signature={}".format(HASHING_ALGORITHM, self.access_key, scope, signed_headers, signature)
    headers = {'X-Amz-Date': amz_timestamp, 'Authorization': authz_header}
    req.headers.update(headers)

    return req


def get_access_token():
    signed_session = requests.session()
    signed_session.auth = AwsV4Auth(ACCESS_KEY, SECRET_KEY, HOSTNAME, "POST")
    resp = signed_session.post(f'https://{HOSTNAME}/authn/tokenExchange')
    respJson = resp.json()
    access_token, refresh_token = respJson["data"]["access_token"], respJson["data"]["refresh_token"]
    print(f'Access token: {access_token}')
    return access_token


# example request to list data services in the environment
def list_data_services():
    access_token = get_access_token()
    resp = requests.get(f'https://{HOSTNAME}/api/v1/organizations/', headers={'Authorization': f'Bearer {access_token}'})
    print(resp.json())


if __name__ == "__main__":
    # get_access_token()
    list_data_services()
