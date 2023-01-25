from typing import List

source_bucket = "bucket"
destination_bucket = source_bucket
database = "database"
date = "2021-01-01"
hours = [str(x).zfill(2) for x in range(21, 23)]


def do_work(table: str, category: List[str] = []) -> None:
    print(f"\n{table}")
    input_path = f"{database}/{table}/date_={date}"
    output_hourly_path = f"{database}/{table}/date_={date}/hour=23"
    if table == "sessions_ab_test":
        for hour in hours:
            for src in category:
                ip = f"{input_path}/hour={hour}/source={src}/"
                ohp = f"{output_hourly_path}/source={src}/"
                print(f"ip: {ip}\tohp: {ohp}")
    elif not category:
        for hour in hours:
            ip = f"{input_path}/hour={hour}/"
            ohp = f"{output_hourly_path}/"
            print(f"ip: {ip}\tohp: {ohp}")
    elif isinstance(category, list) and len(category) > 0:
        for hour in hours:
            for cat in category:
                for to_copy in ["0", "1"]:
                    ip = f"{database}/{table}/category={cat}/date_={date}/hour={hour}/to_copy={to_copy}/ "
                    ohp = f"{database}/{table}/category={cat}/date_={date}/hour=23/to_copy={to_copy}/"
                    print(f"ip: {ip}\tohp: {ohp}")

    print("\n")


def array_work(table: str, category: List[str] = []) -> None:
    hourly_paths = [
        f"{database}/{table}/category={c}/date_={date}/hour={h}/to_copy={t}"
        for c in category
        for h in hours
        for t in [0, 1]
    ]
    s = "\n".join(hourly_paths)
    print(f"{s}")


if __name__ == "__main__":
    categories = ["ad_event", "page_event", "page_view"]
    sources = list(map(lambda x: x.split("_")[-1], categories[1:3]))
    do_work("sessions_ab_test", sources)
    do_work("email_events")
    do_work("page_events", categories)
    array_work("page_events", categories)
