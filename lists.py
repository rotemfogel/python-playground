# List MariaDB table to dump to S3 with an indication to copy to Vertica
from operator import itemgetter

from airfart.utils.chunk import chunk_fn

dump_tables = [
    ("additional_reg_infos", False),
    ("articles", False),
    ("articles_extra_data", False),
    ("article_tags", False),
    ("comments", True),
    ("discussions", False),
    ("instablog_posts", False),
    ("market_currents", False),
    ("moderated_users", False),
    ("moderation_logs", False),
    ("proofreads", False),
    ("sentiments", False),
    ("trackings", False),
    ("transcripts", False),
    ("user_registration_trackings", True),
    ("authors", False),
    ("author_researches", False),
    ("author_research_categories", True),
    ("user_author_research_discounts", True),
    ("reviews", False),
    ("users", False),
    ("tags", False),
    ("mp_user_managements", False),
    ("daily_quotes", False),
    ("links", False),
    ("dividends_lists", False),
    ("etf_categories", False),
    ("etf_tag_extras", False),
    ("bee_volume_controller_incoming_feeds", False),
    ("bee_partners", False),
    ("subscriptions", False),
    ("user_tags", False),
    ("ab_tests", False),
    ("mpg_stale_stocks", True),
    ("ticker_ratings", False),
    ("likes", False),
    ("portfolios", False),
    ("portfolio_tags", False),
    ("streams", False),
    ("author_research_settings", False),
    ("user_followings", False),
    ("author_types", False),
    ("push_subscriptions", False),
    ("tag_redirections", False),
    ("ticker_data", False),
    ("user_disallow_emails", False),
    ("author_whitelist_histories", False),
    ("article_scorings", False),
    ("stock_talks", False),
    ("vocations", False),
    ("pipelines", False),
    ("pipeline_notes", False),
    ("disclosures", False),
    ("disclosure_templates", False),
    ("marketplace_wildcard_banners", False),
    ("marketplace_banner_campaigns", False),
]

priority_tables = [
    "articles",
    "articles_extra_data",
    "comments",
    "daily_quotes_sensor",
    "discussions",
    "instablog_posts",
    "likes",
    "market_currents",
    "portfolios",
    "proofreads",
    "tags",
    "user_followings",
    "users",
]

tables = []
for table, vertica_ind in dump_tables:
    priority_weight = 1
    if table in priority_tables:
        priority_weight = 10
    tables.append(
        {"table": table, "priority_weight": priority_weight, "vertica_ind": vertica_ind}
    )

# order by higher priority first
tables = sorted(tables, key=itemgetter("priority_weight"), reverse=True)

# bucket to arrays
table_chunks = chunk_fn(tables, len(priority_tables))

length = len(table_chunks)
global_tasks = []

for chunks in range(length):
    tasks = []
    completed = "completed_{}".format(chunks)
    for table_to_dump in table_chunks[chunks]:
        table = table_to_dump["table"]
        tasks.append((table, completed))
    print(tasks)
    global_tasks.append(tasks)

prev_chunk = None
for tasks_chunk in global_tasks:
    for task in tasks_chunk:
        if prev_chunk:
            for prev_task in prev_chunk:
                print("%s -> %s" % (prev_task[-1], task[0]))
        else:
            print("maria_dim_sensor -> {}".format(task[0]))
    prev_chunk = tasks_chunk
