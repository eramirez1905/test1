# Delivery Hero Logistics Datasets

The Delivery Hero Logistics and RPS teams are planning to share most of the raw data coming from all Logistics and RPS applications with all the country teams.

The data is published as BigQuery datasets, which you can either directly query from your own GCP projects or you can setup an ETL into your own data warehouse solution.

The data is currently pulled and published 6 times a day. We do not yet have an SLA in place, but data for the previous day of operations should generally be available the next morning. In addition, partial live stats from the current date should also be available.

If data is missing, please use the following form to submit any data related request: [Logistics Request Form](https://product.deliveryhero.net/contact-logistics-rider-dep/). Once submitted, this will create a ticket in our board and will be planned in the following Sprint. Therefore, its resolution could take, at most, a couple of weeks.

The datasets will include data from all of the [Logistics apps](https://product.deliveryhero.net/global-logistics/documentation/) and RPS services:


###Logistics 
* Hurrier
* Rooster
* Porygon
* Roadrunner
* Arara
* Cash Collection
* TES
* Delivery Areas

###RPS
* [OPA & OMA](https://oma.stg.restaurant-partners.com/manual/index.html) - New RPS transmission service
* iCash - Legacy RPS transmission service - **to be deprecated**
* [GoDroid](https://product.deliveryhero.net/global-vendor/documentation/#go-droid) - RPS Native android client
* [GoWin](https://product.deliveryhero.net/global-vendor/documentation/#go-win) - RPS Wrapper/Shell client. Available currently on WEB, WINDOWS and ANDROID.
* [Vendor Monitor](https://product.deliveryhero.net/global-vendor/documentation/about-restaurant-monitor/) - Service used to monitor device connectivity and compliance with actions including auto-closing, auto-calls and salesforce task creation.
* [POS Middleware](https://product.deliveryhero.net/global-vendor/documentation/intro-to-mw/) - Our POS solution where we can integrate with hundreds of POS solutions worldwide.
* [SOTI Mobile Device Management](https://product.deliveryhero.net/global-vendor/documentation/#mobilock-mobicontrol) - This service manages our Android restaurant devices including settings, apps, branding, and updates.
* Device Management System (DMS)

## Continue with...

* [Entity documentation](curated_data/log/overview.md)
* [Changelog](curated_data/log/CHANGELOG.md)
