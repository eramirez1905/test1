from configs.bigquery.datasets.constants import (
    ADYEN_DATASET,
    ADYEN_LATEST_DATASET,
    CLARISIGHTS_DATASET,
    CLARISIGHTS_LATEST_DATASET,
    COMP_INTEL_REPORT_DATASET,
    COMP_INTEL_REPORT_LATEST_DATASET,
    CYBERSOURCE_DATASET,
    CYBERSOURCE_LATEST_DATASET,
    DYNAMODB_DATASET,
    GOOGLE_SHEET_DATASET,
    INTELLIGENCE_LAYER_CCN_DATASET,
    INTELLIGENCE_LAYER_CCN_LATEST_DATASET,
    INTELLIGENCE_LAYER_DATASET,
    INTELLIGENCE_LAYER_LATEST_DATASET,
    MERGE_LAYER_BACKEND_DATASET,
    MERGE_LAYER_BACKEND_INC_DATASET,
    MERGE_LAYER_BACKEND_LATEST_DATASET,
    MERGE_LAYER_BILLING_DATASET,
    MERGE_LAYER_BILLING_LATEST_DATASET,
    MERGE_LAYER_CORPORATE_DATASET,
    MERGE_LAYER_CORPORATE_LATEST_DATASET,
    MERGE_LAYER_IMAGES_DATASET,
    MERGE_LAYER_IMAGES_LATEST_DATASET,
    MERGE_LAYER_JOKER_DATASET,
    MERGE_LAYER_JOKER_LATEST_DATASET,
    MERGE_LAYER_LOYALTY_DATASET,
    MERGE_LAYER_LOYALTY_LATEST_DATASET,
    MERGE_LAYER_PAYMENT_DATASET,
    MERGE_LAYER_PAYMENT_LATEST_DATASET,
    MERGE_LAYER_REFER_A_FRIEND_DATASET,
    MERGE_LAYER_REFER_A_FRIEND_LATEST_DATASET,
    MERGE_LAYER_REWARD_DATASET,
    MERGE_LAYER_REWARD_LATEST_DATASET,
    MERGE_LAYER_SUBSCRIPTION_DATASET,
    MERGE_LAYER_SUBSCRIPTION_LATEST_DATASET,
    NCR_RESTAURANT_PORTAL_DATASET,
    NCR_RESTAURANT_PORTAL_LATEST_DATASET,
    CURATED_DATASET,
    INTERMEDIATE_DATASET,
    LIVE_DATASET,
    REPORT_DATASET,
    PAYPAL_DATASET,
    PAYPAL_LATEST_DATASET,
)
from configs.bigquery.tables.curated import CURATED_TABLES
from configs.bigquery.tables.intermediate import INTERMEDIATE_TABLES
from configs.bigquery.tables.raw.adyen import ADYEN_TABLES
from configs.bigquery.tables.raw.billing import BILLING_TABLES
from configs.bigquery.tables.raw.clarisights import CLARISIGHTS_TABLES
from configs.bigquery.tables.raw.corporate import CORPORATE_TABLES
from configs.bigquery.tables.raw.cybersource import CYBERSOURCE_TABLES
from configs.bigquery.tables.raw.google_sheet import GOOGLE_SHEET_TABLES
from configs.bigquery.tables.raw.images import IMAGES_TABLES
from configs.bigquery.tables.raw.intelligence_layer import INTELLIGENCE_LAYER_TABLES
from configs.bigquery.tables.raw.intelligence_layer_contact_center import (
    INTELLIGENCE_LAYER_CCN_TABLES,
)
from configs.bigquery.tables.raw.joker import JOKER_TABLES
from configs.bigquery.tables.raw.loyalty import LOYALTY_TABLES
from configs.bigquery.tables.raw.payment import PAYMENT_TABLES
from configs.bigquery.tables.raw.pandora import PANDORA_TABLES
from configs.bigquery.tables.raw.refer_a_friend import REFER_A_FRIEND_TABLES
from configs.bigquery.tables.raw.reward import REWARD_TABLES
from configs.bigquery.tables.raw.subscription import SUBSCRIPTION_TABLES
from configs.bigquery.tables.raw.ncr_restaurant_portal import (
    NCR_RESTAURANT_PORTAL_TABLES,
)
from configs.bigquery.tables.raw.paypal import PAYPAL_TABLES
from configs.constants import (
    DATA_ADVANCED_USERS_EMAIL,
    DATA_MEMBERS_EMAIL,
    DATA_USERS_EMAIL,
)
from configs.constructors.dataset import (
    BigQueryDataset,
    BigQueryConfig,
    DatasetAccess,
    TemporaryBigQueryDataset,
    UserAccess,
)


RAW_DATASET_ACCESS = DatasetAccess(
    read=UserAccess(group_emails=(DATA_MEMBERS_EMAIL, DATA_ADVANCED_USERS_EMAIL))
)

CURATED_DATASET_ACCESS = DatasetAccess(
    read=UserAccess(
        group_emails=(DATA_MEMBERS_EMAIL, DATA_USERS_EMAIL, DATA_ADVANCED_USERS_EMAIL),
    ),
)


BQ_CONFIG = BigQueryConfig(
    datasets=(
        TemporaryBigQueryDataset(name=ADYEN_DATASET, access=RAW_DATASET_ACCESS),
        TemporaryBigQueryDataset(name=CLARISIGHTS_DATASET, access=RAW_DATASET_ACCESS),
        TemporaryBigQueryDataset(
            name=COMP_INTEL_REPORT_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(name=CYBERSOURCE_DATASET, access=RAW_DATASET_ACCESS),
        TemporaryBigQueryDataset(name=DYNAMODB_DATASET, access=RAW_DATASET_ACCESS),
        TemporaryBigQueryDataset(
            name=INTELLIGENCE_LAYER_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=INTELLIGENCE_LAYER_CCN_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_BACKEND_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_BACKEND_INC_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_BILLING_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_CORPORATE_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_IMAGES_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_JOKER_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_LOYALTY_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_PAYMENT_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_REFER_A_FRIEND_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_REWARD_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=MERGE_LAYER_SUBSCRIPTION_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(
            name=NCR_RESTAURANT_PORTAL_DATASET, access=RAW_DATASET_ACCESS
        ),
        TemporaryBigQueryDataset(name=PAYPAL_DATASET, access=RAW_DATASET_ACCESS),
        BigQueryDataset(
            name=ADYEN_LATEST_DATASET, access=RAW_DATASET_ACCESS, tables=ADYEN_TABLES
        ),
        BigQueryDataset(
            name=CLARISIGHTS_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=CLARISIGHTS_TABLES,
        ),
        BigQueryDataset(
            name=COMP_INTEL_REPORT_LATEST_DATASET, access=RAW_DATASET_ACCESS
        ),
        BigQueryDataset(
            name=CYBERSOURCE_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=CYBERSOURCE_TABLES,
        ),
        BigQueryDataset(
            name=GOOGLE_SHEET_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=GOOGLE_SHEET_TABLES,
        ),
        BigQueryDataset(
            name=INTELLIGENCE_LAYER_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=INTELLIGENCE_LAYER_TABLES,
        ),
        BigQueryDataset(
            name=INTELLIGENCE_LAYER_CCN_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=INTELLIGENCE_LAYER_CCN_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_BACKEND_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=PANDORA_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_BILLING_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=BILLING_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_CORPORATE_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=CORPORATE_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_IMAGES_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=IMAGES_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_JOKER_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=JOKER_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_LOYALTY_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=LOYALTY_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_PAYMENT_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=PAYMENT_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_REFER_A_FRIEND_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=REFER_A_FRIEND_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_REWARD_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=REWARD_TABLES,
        ),
        BigQueryDataset(
            name=MERGE_LAYER_SUBSCRIPTION_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=SUBSCRIPTION_TABLES,
        ),
        BigQueryDataset(
            name=NCR_RESTAURANT_PORTAL_LATEST_DATASET,
            access=RAW_DATASET_ACCESS,
            tables=NCR_RESTAURANT_PORTAL_TABLES,
        ),
        BigQueryDataset(
            name=CURATED_DATASET, tables=CURATED_TABLES, access=CURATED_DATASET_ACCESS
        ),
        BigQueryDataset(
            name=INTERMEDIATE_DATASET,
            access=CURATED_DATASET_ACCESS,
            tables=INTERMEDIATE_TABLES,
        ),
        BigQueryDataset(name=LIVE_DATASET, access=CURATED_DATASET_ACCESS),
        BigQueryDataset(name=REPORT_DATASET, access=CURATED_DATASET_ACCESS),
        BigQueryDataset(
            name=PAYPAL_LATEST_DATASET, access=RAW_DATASET_ACCESS, tables=PAYPAL_TABLES
        ),
    )
)
