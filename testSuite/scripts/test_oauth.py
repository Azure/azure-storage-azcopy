import utility as util
import os
import os.path
import time

# test oauth login with default parameters
def test_login_with_default():
    # execute the azcopy login.
    output = util.Command("login").execute_azcopy_command_interactive()
    if output is None:
        print("error login")
        print("test_login_with_default test failed")
        return

    # for windows, further check access token file, for other os, report success if login succeeded.
    if os.name == 'nt':
        # get the job Id of new job started by parsing the azcopy console output.
        output = util.Command("info").add_arguments("AzCopyAppPath").execute_azcopy_info()
        if output is None:
            print("error get info")
            print("test_login_with_default test internal error, fail to validate login")

        token_file_path = os.path.join(output, "AccessToken.json")
        if not os.path.isfile(token_file_path):
            print("cannot find cached AccessToken.json")
            print("test_login_with_default test failed")
            return

        # check access token should be refreshed. 5 minutes should be enough for manual operations.
        if time.time() - os.stat(token_file_path).st_mtime < 30:
            print("test_login_with_default passed successfully")
        else:
            print("test_login_with_default test failed")
    else:
        print("test_login_with_default passed successfully")

# test oauth login with customized tenant and aad endpoint
def test_login(tenant, aadEndpoint):
    print("test_login tenant: ", tenant , " aadEndpoint: ", aadEndpoint)

    # execute the azcopy login.
    cmd = util.Command("login")
    if tenant != "":
        cmd.add_flags("tenant-id", tenant)
    if aadEndpoint != "":
        cmd.add_flags("aad-endpoint", aadEndpoint)
    output = cmd.execute_azcopy_command_interactive()
    if output is None:
        print("error login")
        print("test_login test failed")
        return

    # for windows, further check access token file, for other os, report success if login succeeded.
    if os.name == 'nt':
        # get the job Id of new job started by parsing the azcopy console output.
        output = util.Command("info").add_arguments("AzCopyAppPath").execute_azcopy_info()
        if output is None:
            print("error get info")
            print("test_login test internal error, fail to validate login")

        token_file_path = os.path.join(output, "AccessToken.json")
        if not os.path.isfile(token_file_path):
            print("cannot find cached AccessToken.json")
            print("test_login test failed")
            return

        # check access token should be refreshed. 5 minutes should be enough for manual operations.
        if time.time() - os.stat(token_file_path).st_mtime < 30:
            print("test_login passed successfully")
        else:
            print("test_login test failed")
    else:
        print("test_login passed successfully")

# test oauth logout
def test_logout():
    print("test_logout")
    # execute the azcopy login.
    output = util.Command("logout").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error logout")
        print("test_logout test failed")
        return

    # for windows, further check access token file, for other os, report success if login succeeded.
    if os.name == 'nt':
        # get the job Id of new job started by parsing the azcopy console output.
        output = util.Command("info").add_arguments("AzCopyAppPath").execute_azcopy_info()
        if output is None:
            print("error get info")
            print("test_logout test internal error, fail to validate logout")

        print("test_logout AzCopyAppPath detected ", output)

        token_file_path = os.path.join(output, "AccessToken.json")
        if os.path.isfile(token_file_path):
            print("find cached AccessToken.json after logout")
            print("test_logout test failed")
        else:
            print("test_logout passed successfully")
    else:
        print("test_logout passed successfully")
