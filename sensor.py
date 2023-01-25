def get_dependencies(dependencies, params=None):
    """
    A function to set sensors based on dependencies provided

    Returns an array of sensors

    :param params:
    :param dependencies: A dict of schemas and tables
    :type dependencies: dic

    """

    sensors_config = {
        "maria_dim": {
            "task_id": "maria_dim_sensor",
            "external_dag_id": "emr_maria_dim",
            "external_task_id": "end_of_dag",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "maria_dim_incr": {
            "task_id": "maria_dim_incr_sensor",
            "external_dag_id": "emr_maria_dim_incr",
            "external_task_id": "end_of_dag",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 2
            * 60
            * 60,  # although one should be enough according the previous runs
        },
        "contents": {
            "task_id": "contents_sensor",
            "external_dag_id": "athena_incr_contents.athena_contents",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "contents_agg": {
            "task_id": "contents_agg_sensor",
            "external_dag_id": "athena_incr_contents_agg.athena_contents_agg",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "content_tags": {
            "task_id": "content_tags_sensor",
            "external_dag_id": "athena_snap_content_tags.athena_content_tags",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "sessions_ab_test_maria_dim": {
            "task_id": "sessions_ab_test_maria_dim_sensor",
            "external_dag_id": "athena_incr_sessions_ab_test.athena_sessions_ab_test_maria_dim_ds",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "sessions_ab_test_page_event": {
            "task_id": "sessions_ab_test_maria_dim_sensor",
            "external_dag_id": "athena_incr_sessions_ab_test.athena_sessions_ab_test_page_event_ds",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "authors": {
            "task_id": "authors_sensor",
            "external_dag_id": "athena_snap_authors.athena_authors",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "session": {
            "task_id": "session_sensor",
            "external_dag_id": "session_interim_tables",
            "external_task_id": "add_partition",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "user_followings": {
            "task_id": "user_followings_sensor",
            "external_dag_id": "athena_snap_user_followings.athena_user_followings",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "tickers": {
            "task_id": "tickers_sensor",
            "external_dag_id": "athena_snap_tickers",
            "external_task_id": "athena_tickers",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "content_disclosures": {
            "task_id": "content_disclosures_sensor",
            "external_dag_id": "content_disclosures",
            "external_task_id": "end_of_dag",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "premium_crosssell_mpsignup": {
            "task_id": "premium_crosssell_followup_1_sensor",
            "external_dag_id": "athena_production_tables.athena_premium_crosssell_mpsignup",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 3 * 60 * 60,
        },
        "paying_subscriptions": {
            "task_id": "paying_subscriptions_sensor",
            "external_dag_id": "paying_subscriptions.athena_paying_subscriptions",
            "external_task_id": "completed",
            "poke_interval": 90,
            "mode": "reschedule",
            "timeout": 2 * 60 * 60,
        },
    }

    templated_dags = [
        "marketplace_services",
        "zuora_subscriptions",
        "active_users",
        "subscription_events",
        "users",
        "portfolios",
    ]
    for dag in templated_dags:
        sensors_config.update(
            {
                dag: {
                    "task_id": "{dag}_sensor".format(dag=dag),
                    "external_dag_id": "{dag}.athena_{dag}".format(dag=dag),
                    "external_task_id": "completed",
                    "poke_interval": 90,
                    "mode": "reschedule",
                    "timeout": 3 * 60 * 60,
                }
            }
        )

    sensors = {}

    for schema in [
        "dbr",
        "dbl",
        "mariadb",
        "zuora",
        "zuora_stateful",
        "dbl_backfill",
        "for_sapi",
    ]:
        for table in dependencies.get(schema, []):
            if table in ("page_events", "email_events"):
                eval_schema = (
                    lambda s: table
                    if schema == "dbl"
                    else table + "_hist"
                    if schema == "dbl_backfill"
                    else None
                )
                sensors.update(
                    {
                        "{}.{}".format(schema, table): {
                            "task_id": "{}_sensor".format(eval_schema(schema)),
                            "external_dag_id": "emr_{}".format(eval_schema(schema)),
                            "external_task_id": "check_temp_results",
                            "poke_interval": 90,
                            "mode": "reschedule",
                            "timeout": 3 * 60 * 60,
                        }
                    }
                )
            elif schema in ["dbr", "dbl", "for_sapi"]:
                if table == "sessions_ab_test":
                    sensors.update(
                        {
                            "{}.{}".format(schema, table): sensors_config[
                                "sessions_ab_test_maria_dim"
                            ]
                        }
                    )
                    sensors.update(
                        {
                            "{}.{}".format(schema, table): sensors_config[
                                "sessions_ab_test_page_event"
                            ]
                        }
                    )
                else:
                    sensors.update(
                        {"{}.{}".format(schema, table): sensors_config[table]}
                    )
            elif schema == "mariadb":
                # mariadb dependencies
                sensors.update(
                    {
                        "{}.{}".format(schema, table): {
                            "task_id": "mariadb_" + table + "_sensor",
                            "external_dag_id": "mariadb_dump.athena_" + table,
                            "external_task_id": "completed",
                            "poke_interval": 90,
                            "mode": "reschedule",
                            "timeout": 2 * 60 * 60,
                        }
                    }
                )
            elif schema == "zuora":
                # zuora dependencies
                sensors.update(
                    {
                        "{}.{}".format(schema, table): {
                            "task_id": "zuora_sensor",
                            "external_dag_id": "zuora_json",
                            "external_task_id": "export_completed",
                            "poke_interval": 90,
                            "mode": "reschedule",
                            "timeout": 1 * 60 * 60,
                        }
                    }
                )
            elif schema == "zuora_stateful":
                # zuora dependencies
                sensors.update(
                    {
                        "{}.{}".format(schema, table): {
                            "task_id": "zuora_sensor",
                            "external_dag_id": "zuora_dump",
                            "external_task_id": "record_last_partition",
                            "poke_interval": 90,
                            "mode": "reschedule",
                            "soft_fail": True,
                            "timeout": 1 * 60 * 60,
                        }
                    }
                )

    if params:
        for table, new_params in params.items():
            if new_params.pop("depends_on_hourly", None):
                new_params.update({"execution_date_fn": "hourly_fn"})
            if new_params.pop("depends_on_daily", None):
                new_params.update({"execution_date_fn": "daily_fn"})
            if new_params.pop("depends_on_mariadb", None):
                new_params.update({"execution_date_fn": "mariadb_fn"})
            timeout = new_params.pop("timeout", None)
            if timeout:
                new_params.update({"timeout": timeout})
            sensors[table].update(new_params)

    return sensors


dependencies = {
    "mariadb": ["marketplace_wildcard_banners", "marketplace_banner_campaigns"]
}
dependencies_params = {
    "mariadb.marketplace_wildcard_banners": {
        "depends_on_hourly": True,
        "timeout": 3 * 60 * 60,
    },
    "mariadb.marketplace_banner_campaigns": {
        "depends_on_hourly": True,
        "timeout": 3 * 60 * 60,
    },
}
sensor = get_dependencies(dependencies, dependencies_params)
print(sensor)
