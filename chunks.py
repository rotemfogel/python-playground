queries = [
    {
        "name": "account",
        "query": "select * from account",
    },
    {
        "name": "amendment",
        "query": "select * from amendment",
    },
    {
        "name": "contact",
        "query": "select * from contact",
    },
    # remove until I get a response from zuora support
    # {
    #     "name": "discountappliedmetrics",
    #     "query": "select * from discountappliedmetrics",
    # },
    {
        "name": "payment",
        "query": "select *, account.id, paymentmethod.id from payment",
    },
    {
        "name": "paymentmethod",
        "query": "select * from paymentmethod",
    },
    {
        "name": "paymentmethodsnapshot",
        "query": "select * from paymentmethodsnapshot",
        "deleted": None,
    },
    {
        "name": "product",
        "query": "select * from product",
    },
    {
        "name": "productrateplan",
        "query": "select *, product.id from productrateplan",
    },
    {
        "name": "productrateplancharge",
        "query": "select *, productrateplan.id  from productrateplancharge",
    },
    {
        "name": "productrateplanchargetier",
        "query": "select *, productrateplancharge.id from productrateplanchargetier",
    },
    {
        "name": "rateplan",
        "query": "select *, Amendment.Id, subscription.id, productrateplan.id from rateplan",
    },
    {
        "name": "rateplancharge",
        "query": "select *, rateplan.id, productrateplancharge.id from rateplancharge",
    },
    {
        "name": "rateplanchargetier",
        "query": "select *, rateplancharge.id from rateplanchargetier",
    },
    {
        "name": "refund",
        "query": "select *, account.id from refund",
    },
    {
        "name": "refundinvoicepayment",
        "query": "select *, invoice.id, invoicepayment.id, refund.id from refundinvoicepayment",
    },
    {
        "name": "invoice",
        "query": "select *, account.id from invoice",
    },
    {
        "name": "invoiceitem",
        "query": "select *, invoice.id, RatePlanCharge.Id from invoiceitem",
    },
    {
        "name": "invoicepayment",
        "query": "select *, invoice.id, payment.id from invoicepayment",
    },
    {
        "name": "subscription",
        "query": "select *, account.id from subscription",
    },
]
max_batch_size = 5
query_chunks = [
    queries[i * max_batch_size : (i + 1) * max_batch_size]
    for i in range((len(queries) + max_batch_size - 1) // max_batch_size)
]
length = len(query_chunks)
for chunks in range(length):
    for query in query_chunks[chunks]:
        table = query["name"].lower()
        print(table)
    print(chunks)
