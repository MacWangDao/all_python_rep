from django.contrib.auth.hashers import make_password
from django.test import TestCase

# Create your tests here.

from django.conf import settings

settings.configure(DEBUG=True)
'''
$2b$12$qoDvLwXxzSEe.YTxouabFeAxGZz8nFTmanBG0HjdRmArUWrboQR4e
'django.contrib.auth.hashers.BCryptPasswordHasher',
    'django.contrib.auth.hashers.SHA1PasswordHasher',
    'django.contrib.auth.hashers.Argon2PasswordHasher',
    'django.contrib.auth.hashers.PBKDF2PasswordHasher',
    'django.contrib.auth.hashers.PBKDF2SHA1PasswordHasher',
    'django.contrib.auth.hashers.BCryptSHA256PasswordHasher',

'''
pwd = 'admin@1234'
# mpwd1 = make_password(pwd, hasher='pbkdf2_sha256')
mpwd1 = make_password(pwd, hasher='bcrypt_sha256')
print(mpwd1)
