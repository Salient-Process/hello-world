<<<<<<< HEAD
#git config --global user.email fpinzonperalta@salientprocess.com
#git config --global user.email bp_francisco_pinzon@colpal.com
=======
>>>>>>> 08d2b74382f4a2602813c1c52b8b17e64b0582c9
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="HttpTrigger1")
@app.route(route="req")
def main(req: func.HttpRequest) -> str:
    user = req.params.get("user")
<<<<<<< HEAD
    return f"Hello, {user}!"
=======
    return f"Hello, {user}!"
>>>>>>> 08d2b74382f4a2602813c1c52b8b17e64b0582c9
