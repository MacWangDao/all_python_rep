import base64

from jose import jwt
from datetime import datetime, timedelta


def encode_token():
    payload = {
        'exp': datetime.now() + timedelta(minutes=30),  # 令牌过期时间
        'username': 'BigFish',  # 想要传递的信息,如用户名ID
        'sub': '1'
    }
    key = """{"kty":"RSA","kid":"5995191754202518511","alg":"RS256","n":"lSbCWJdg-73rjKfV5ZijV0dyxZXnH-Ev3eVgkkZbab9xelhTlExdnBKDD1d6F9C_ZkvyV3tOg1tcD27iKZ4WD2Xy6vvUvNj6fxtbcgKOQnW2G5l2aJ69EegXE6RG9yc6EjKX69361t8pZ76MWFQ3BNt-X-6ZqQRJgrzDGD5pTMXHaZceuIIA3blVtBIylKmowt2ug6ieKTMk6CEzdDetBDSeGnXc8vgNQa3TzSvYW4wRVmCIYVby_qKUQqqAnWw1RtrtnLNrp0cJHiCf05g7eRttc4xHA0u4GnhlJEuEVQSr_7DC3tEMcrdH30l_PqO6FHs8DoEn6LrFCsG1oSza2w","e":"AQAB"}"""
    encoded_jwt = jwt.encode(payload, key, algorithm='HS256')
    print(encoded_jwt)
    return encoded_jwt


def decode_token(encoded_jwt):
    key = """{"kty":"RSA","kid":"5995191754202518511","alg":"RS256","n":"lSbCWJdg-73rjKfV5ZijV0dyxZXnH-Ev3eVgkkZbab9xelhTlExdnBKDD1d6F9C_ZkvyV3tOg1tcD27iKZ4WD2Xy6vvUvNj6fxtbcgKOQnW2G5l2aJ69EegXE6RG9yc6EjKX69361t8pZ76MWFQ3BNt-X-6ZqQRJgrzDGD5pTMXHaZceuIIA3blVtBIylKmowt2ug6ieKTMk6CEzdDetBDSeGnXc8vgNQa3TzSvYW4wRVmCIYVby_qKUQqqAnWw1RtrtnLNrp0cJHiCf05g7eRttc4xHA0u4GnhlJEuEVQSr_7DC3tEMcrdH30l_PqO6FHs8DoEn6LrFCsG1oSza2w","e":"AQAB"}"""
    res = jwt.decode(encoded_jwt, key, algorithms='HS256', options={"verify_signature": False})
    print(res)


def wtest_decode():
    header = base64.b64decode('eyJhbGciOiJSUzI1NiIsImtpZCI6IjU5OTUxOTE3NTQyMDI1MTg1MTEifQ')
    payload = base64.b64decode(
        'eyJlbWFpbCI6IjEzMjM0MjFAcXEuY29tIiwibmFtZSI6Im1haWNlIiwibW9iaWxlIjpudWxsLCJleHRlcm5hbElkIjoiODk2OTM3MDYzMjU0MTE2MjEzIiwidWRBY2NvdW50VXVpZCI6ImQ4Nzc1NDBhN2E3N2Y0NzI5NmVjZmY5OTA3OTQzNzQ0U2tOYnhVTEptNXkiLCJvdUlkIjoiMjMyMDE4OTI4MDc2MDk5MzY1MSIsIm91TmFtZSI6IumVv-S4ieinkiIsIm9wZW5JZCI6bnVsbCwiaWRwVXNlcm5hbWUiOiJtYWljZSIsInVzZXJuYW1lIjoibWFpY2UiLCJhcHBsaWNhdGlvbk5hbWUiOiLmmbrog73pgInlnYBf6KeE5YiS566h55CGIiwiZW50ZXJwcmlzZUlkIjoiaWRhYXMiLCJpbnN0YW5jZUlkIjoiaWRhYXMiLCJhbGl5dW5Eb21haW4iOiIiLCJleHRlbmRGaWVsZHMiOnsidGhlbWVDb2xvciI6ImdyZWVuIiwiYXBwTmFtZSI6IuaZuuiDvemAieWdgF_op4TliJLnrqHnkIYifSwiZXhwIjoxNjQ2MTI0Mjc2LCJqdGkiOiIwZ3RmV3pxTHVvckc1M09jVUtZTkhBIiwiaWF0IjoxNjQ2MTIzNjc2LCJuYmYiOjE2NDYxMjM2MTYsInN1YiI6Im1haWNlIiwiaXNzIjoiaHR0cDovLzEwLjkwLjUuOTAvIiwiYXVkIjoiaWRhYXNwbHVnaW5fand0MTQifQ')
    # unsignedToken = encodeBase64(header) + '.' + encodeBase64(payload)
    # signature = HMAC - SHA256(key, unsignedToken)
    print(header)
    print(payload)


if __name__ == '__main__':
    encoded_jwt = encode_token()
    encoded_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NzA1OTUxMjYsInVzZXJuYW1lIjoiQmlnRmlzaCIsInN1YiI6IjEifQ.IYiaNrb2iR8kjbMFhY1Sjkm7KObcaq3d15owgrt6WJ8"
    decode_token(encoded_jwt)
    # test_decode()
