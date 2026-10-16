"""Synthetic pagination example; start examples.demo_server before the Worker."""
from spider_man import Item, Crawler, Request, Seed

app = Crawler(project="demo", task="user_post")


@app.on_start
def indexes():
    app.mongo.create_index(app.collection_name, ["user_id", "post_id"], unique=True)


@app.request
def request(seed):
    if not seed.user_id:
        raise ValueError("user_id is required")
    yield Request("http://127.0.0.1:18080/posts", params={"user_id": seed.user_id, "page": seed.page or 1}, seed=seed)


@app.parse
def parse(result):
    body = result.json()
    for post in body["posts"]:
        yield Item(data=post, seed=result.seed)
    if body.get("next_page"):
        yield Seed({"user_id": result.seed.user_id, "page": body["next_page"]}, priority=result.seed.params.priority)


if __name__ == "__main__":
    app.start()
