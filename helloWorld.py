#git config --global user.email fpinzonperalta@salientprocess.com
#git config --global user.email bp_francisco_pinzon@colpal.com
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="HttpTrigger1")
@app.route(route="req")
def main(req: func.HttpRequest) -> str:
    user = req.params.get("user")
    return f"Hello, {user}!"