import utility as util
import os.path
import time

# test_cancel_job verifies the cancel functionality of azcopy
def test_login_default_tenant():
    print("test_login_default_tenant")
    # execute the azcopy login.
    output = util.Command("login").execute_azcopy_command_interactive()
    if output is None:
        print("error login")
        print("test_login_default_tenant test failed")
        return

    # get the job Id of new job started by parsing the azcopy console output.
    output = util.Command("info").add_arguments("AzCopyAppPath").execute_azcopy_info()
    if output is None:
        print("error get info")
        print("test_login_default_tenant test internal error, fail to validate login")

    token_file_path = os.path.join(output, "AccessToken.json")
    if not os.path.isfile(token_file_path):
        print("cannot find cached AccessToken.json")
        print("test_login_default_tenant test failed")
        return

    # check access token should be refreshed. 5 minutes should be enough for manual operations.
    if time.time() - os.stat(token_file_path).st_mtime < 30:
        print("test_login_default_tenant passed successfully")
    else:
        print("test_login_default_tenant test failed")


def test_logout():
    # execute the azcopy login.
    output = util.Command("logout").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error logout")
        print("test_logout test failed")
        return

    # get the job Id of new job started by parsing the azcopy console output.
    output = util.Command("info").add_arguments("AzCopyAppPath").execute_azcopy_info()
    if output is None:
        print("error get info")
        print("test_logout test internal error, fail to validate logout")

    print("AzCopyAppPath ", output)

    token_file_path = os.path.join(output, "AccessToken.json")
    if os.path.isfile(token_file_path):
        print("find cached AccessToken.json after logout")
        print("test_logout test failed")
    else:
        print("test_logout passed successfully")
