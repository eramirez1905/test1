-- Dropping tables in prod as these get truncated and recomputed every day and they
-- are already available in mkt_vendor_user_map schema

DROP TABLE mkt_ncr.tb_customer_orders_coordinates;
DROP TABLE mkt_ncr.tb_polygon_point_map;
DROP TABLE mkt_ncr.tb_restaurants_polygons;
DROP TABLE mkt_ncr.tb_vendor_user_geo_mapping;
DROP TABLE mkt_ncr_dev.tb_customer_orders_coordinates;
DROP TABLE mkt_ncr_dev.tb_polygon_point_map;
DROP TABLE mkt_ncr_dev.tb_restaurants_polygons;
DROP TABLE mkt_ncr_dev.tb_vendor_user_geo_mapping;
