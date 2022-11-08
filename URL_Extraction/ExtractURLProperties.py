from app_logger.logger import APP_LOGGER
from urllib.parse import urlparse,parse_qs
import tldextract
import os
import re
from ipaddress import ip_address,AddressValueError
import pandas as pd
import subprocess
class extract_url_properties:
    def __init__(self,url):
        self.input_url=url
        self.fileObject=open("Log_Files_Collection/Prediction_Logs/Result_Log.txt","a+")
        self.log=APP_LOGGER()


    def load_user_data(self):
        try:
            self.log.log(self.fileObject,'Extracting the url properties!!')
            if urlparse(self.input_url).scheme in ['http','https','ftp']:
                url_domain=urlparse(self.input_url).netloc
                if tldextract.extract(self.input_url).suffix =='':
                    url_TLD=tldextract.extract(self.input_url).domain
                else:
                    url_TLD=tldextract.extract(self.input_url).suffix
                url_directory = os.path.dirname(urlparse(self.input_url).path)
                url_files=os.path.basename(urlparse(self.input_url).path)
                url_parameter=urlparse(self.input_url).query
                url_parameter_count= len(parse_qs(urlparse(self.input_url).query))
                if tldextract.extract(url_parameter).suffix =='':
                    tld_present_params = 0
                url_prop = {'qty_dot_url':[self.input_url.count('.')],'qty_hyphen_url':[self.input_url.count('.')],'qty_underline_url':[self.input_url.count('_')],'qty_slash_url':[self.input_url.count('/')],'qty_questionmark_url':[self.input_url.count('?')],'qty_equal_url':[self.input_url.count('=')],'qty_at_url':[self.input_url.count('@')],'qty_and_url':[self.input_url.count('&')],'qty_exclamation_url':[self.input_url.count('!')],'qty_space_url':[self.input_url.count(' ')],'qty_tilde_url':[self.input_url.count('~')],'qty_comma_url':[self.input_url.count('],')],'qty_plus_url':[self.input_url.count('+')],'qty_asterisk_url':[self.input_url.count('*')],'qty_hashtag_url':[self.input_url.count('#')],'qty_dollar_url':[self.input_url.count('$')],'qty_percent_url':[self.input_url.count('%')],'qty_tld_url':[len(url_TLD)],'length_url':[len(self.input_url)],'email_in_url':[self.is_email_present(self.input_url)],'qty_dot_domain':[url_domain.count('.')],'qty_hyphen_domain':[url_domain.count('-')],'qty_underline_domain':[url_domain.count('_')],'qty_slash_domain':[url_domain.count('/')],'qty_questionmark_domain':[url_domain.count('?')],'qty_equal_domain':[url_domain.count('=')],'qty_at_domain':[url_domain.count('@')],'qty_and_domain':[url_domain.count('&')],'qty_exclamation_domain':[url_domain.count('!')],'qty_space_domain':[url_domain.count(' ')],'qty_tilde_domain':[url_domain.count('~')],'qty_comma_domain':[url_domain.count('],')],'qty_plus_domain':[url_domain.count('+')],'qty_asterisk_domain':[url_domain.count('*')],'qty_hashtag_domain':[url_domain.count('#')],'qty_dollar_domain':[url_domain.count('$')],'qty_percent_domain':[url_domain.count('%')],'qty_vowels_domain':[len([each for each in url_domain if each in "aeiouAEIOU"])],'domain_length':[len(url_domain)],'domain_in_ip':[self.is_ip_present(url_domain)],'server_client_domain':[0],'qty_dot_directory':[url_directory.count('.')],'qty_hyphen_directory':[url_directory.count('-')],'qty_underline_directory':[url_directory.count('_')],'qty_slash_directory':[url_directory.count('/')],'qty_questionmark_directory':[url_directory.count('?')],'qty_equal_directory':[url_directory.count('=')],'qty_at_directory':[url_directory.count('@')],'qty_and_directory':[url_directory.count('&')],'qty_exclamation_directory':[url_directory.count('!')],'qty_space_directory':[url_directory.count(' ')],'qty_tilde_directory':[url_directory.count('~')],'qty_comma_directory':[url_directory.count('],')],'qty_plus_directory':[url_directory.count('+')],'qty_asterisk_directory':[url_directory.count('*')],'qty_hashtag_directory':[url_directory.count('#')],'qty_dollar_directory':[url_directory.count('$')],'qty_percent_directory':[url_directory.count('%')],'directory_length':[len(url_directory)],'qty_dot_file':[url_files.count('.')],'qty_hyphen_file':[url_files.count('-')],'qty_underline_file':[url_files.count('_')],'qty_slash_file':[url_files.count('/')],'qty_questionmark_file':[url_files.count('?')],'qty_equal_file':[url_files.count('=')],'qty_at_file':[url_files.count('@')],'qty_and_file':[url_files.count('&')],'qty_exclamation_file':[url_files.count('!')],'qty_space_file':[url_files.count(' ')],'qty_tilde_file':[url_files.count('~')],'qty_comma_file':[url_files.count('],')],'qty_plus_file':[url_files.count('+')],'qty_asterisk_file':[url_files.count('*')],'qty_hashtag_file':[url_files.count('#')],'qty_dollar_file':[url_files.count('$')],'qty_percent_file':[url_files.count('%')],'file_length':[len(url_files)],'qty_dot_params':[url_parameter.count('.')],'qty_hyphen_params':[url_parameter.count('-')],'qty_underline_params':[url_parameter.count('_')],'qty_slash_params':[url_parameter.count('/')],'qty_questionmark_params':[url_parameter.count('?')],'qty_equal_params':[url_parameter.count('=')],'qty_at_params':[url_parameter.count('@')],'qty_and_params':[url_parameter.count('&')],'qty_exclamation_params':[url_parameter.count('!')],'qty_space_params':[url_parameter.count(' ')],'qty_tilde_params':[url_parameter.count('~')],'qty_comma_params':[url_parameter.count('],')],'qty_plus_params':[url_parameter.count('+')],'qty_asterisk_params':[url_parameter.count('*')],'qty_hashtag_params':[url_parameter.count('#')],'qty_dollar_params':[url_parameter.count('$')],'qty_percent_params':[url_parameter.count('%')],'params_length':[len(url_parameter)],'tld_present_params':[tld_present_params],'qty_params':[url_parameter_count],'time_response':[0],'domain_spf':[0],'asn_ip':[0],'time_domain_activation':[0],'time_domain_expiration':[0],'qty_ip_resolved':[0],'qty_nameservers':[0],'qty_mx_servers':[0],'ttl_hostname':[0],'tls_ssl_certificate':[0],'qty_redirects':[0],'url_google_index':[0],'domain_google_index':[0],'url_shortened':[0],'phishing':[0]}
                df = pd.DataFrame.from_dict(url_prop)   
                df.to_csv("Result_Dataset_File/input_data/dataset_user.csv" , index=None, header=True)
                return 'Result_Dataset_File/input_data'
            else:
                return 'invalid url'
        except Exception as e:
            self.log.log(self.fileObject,e)
            raise e

    def createResultDir(self):
        try:
            path=os.path.join('Result_Dataset_File/','input_data/')
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as e:
            file=open('ErrorLogs.txt','a+')
            self.logger.log(file,"Error while creating directory %s:"% e)
            file.close()
            raise OSError

    def is_ip_present(self,str):
        try:
            if(ip_address(str)):
                return 1
        except (AddressValueError, ValueError):
                return  0

    def is_email_present(self,str):
        email_regex = '\S+@\S+' 
        try:
            if(len(re.findall(email_regex,str)) > 0):
                    return 1
            else:
                return  0
        except Exception as e:
            raise e
    # def get_TLL(self):
    #     p = subprocess.Popen(["ping",self.input_url], stdout=subprocess.PIPE)
    #     res=p.communicate()[0]
    #     if p.returncode > 0:
    #         print('server error')
    #     else:
    #         pattern = re.compile('TTL=\d*')
    #         print(pattern.search(str(res)).group())
