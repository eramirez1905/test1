CREATE OR REPLACE VIEW `peyabi`.`vw_bi_partners` AS
select
    `r`.`id`                                                    AS `restaurant_id`,
    `r`.`name`                                                  AS `restaurant`,
    (case
        when isnull(`a`.`area_id`) then 0
        else `a`.`area_id`
    end) AS `area_id`,
    (case
        when isnull(`ar`.`city_id`) then 0
        else `ar`.`city_id`
    end)                                                        AS `city_id`,
    (case
        when isnull(`r`.`country_id`) then 0
        else `r`.`country_id`
    end)                                                        AS `country_id`,
    (case
        when isnull(`r`.`state`) then '0'
        else `r`.`state`
    end)                                                        AS `state`,
    (case
        when isnull(`r`.`type`) then '0'
        else `r`.`type`
    end)                                                        AS `type`,
    `r`.`is_vip`                                                AS `is_vip`,
    (case
        when isnull(`r`.`migration_id`) then 0
        else `r`.`migration_id`
    end)                                                        AS `migration_id`,
    `r`.`registered_date`                                       AS `registered_date`,
    ifnull(date_format(`r`.`registered_date`, '%Y-%m-%d')
                                                , '1900-01-01') AS `registered_date_string`,
    `r`.`date_created`                                          AS `date_created`,
    ifnull(date_format(`r`.`date_created`, '%Y-%m-%d')
                                            , '1900-01-01')     AS `date_created_string`,
    `r`.`is_important_account`                                  AS `is_important_account`,
    `a`.`street`                                                AS `street`,
    `a`.`door_number`                                           AS `door_number`,
    `a`.`phone`                                                 AS `phone`,
    `con`.`name`                                                AS `conctact_name`,
    `con`.`last_name`                                           AS `contact_last_name`,
    `con`.`phones`                                              AS `conctact_phones`,
    (ifnull(`online`.`online_payments`, 0) > 0)                 AS `has_online_payment`,
    (case
        when ((`r`.`stamps_needed` > 0)
        and (`r`.`stamps_state` = 1)) then 1
        else 0
    end)                                                        AS `has_loyalty_program`,
    `rs`.`name`                                                 AS `reception_system_name`,
    ifnull((`rs`.`is_pos` = 1), 0)                              AS `has_pos`,
    `food_category`.`main_cousine`                              AS `main_cousine`,
    ---------------------
    ifnull(`comm`.`cant_com`, 0)                                AS `cant_com`,
    `r`.`talent`                                                AS `talent`,
    (case
        when (isnull(`peyadb`.`menu`.`banner_url`)
        or (length(`peyadb`.`menu`.`banner_url`) < 1)) then 0
        else 1
    end)                                                        AS `has_banner`,
    (case
        when isnull((
        select
            `w`.`discount_value`
        from
            `peyadb`.`weekly_discount` `w`
        where
            ((`r`.`id` = `w`.`restaurant_id`)
            and (`w`.`from_date` <= (curdate() + interval 4 hour))
            and (isnull(`w`.`to_date`)
            or (`w`.`to_date` > (curdate() + interval 4 hour))))
        limit 1)) then 0
        else 1
    end)                                                        AS `has_discount`,
    `dt`.`description`                                          AS `delivery_time`,
    ifnull(`dz`.`min_delivery_amount`, 0)                       AS `min_delivery_amount`,
    ifnull(`dz`.`max_delivery_amount`, 0)                       AS `max_delivery_amount`,
    ifnull(`dz`.`min_shipping_amount`, 0)                       AS `min_shipping_amount`,
    ifnull(`dz`.`max_shipping_amount`, 0)                       AS `max_shipping_amount`,
    ifnull(`dz`.`cantidad_zonas`, 0)                            AS `cantidad_zonas`,
    ifnull(`dt`.`id`, 0)                                        AS `delivery_time_id`,
    ifnull(`dz`.`has_MOV`, 0)                                   AS `has_mov`,
    (case
        when (isnull(`r`.`use_live_order_tracking`)
        or (`r`.`country_id` = 5)) then 0
        else `r`.`use_live_order_tracking`
    end) AS `use_live_order_tracking`,
    `r`.`address_id`                                            AS `address_id`,
    replace(replace(
    (
        case
        when (locate('|', `r`.`disabled_reason`) > 0) then substring_index(substring_index(`r`.`disabled_reason`, '|',-(1)), '|', 1)
        else
        (case
            when (upper(`r`.`disabled_reason`) not in ('CERRÓ SUS PUERTAS DEFINITIVAMENTE',
            'CERRÓ SUS PUERTAS TEMPORALMENTE',
            'CAMBIO DE DUEÑO',
            'DEJÓ DE HACER DELIVERY',
            'NO QUIERE AUTOMATIZARSE',
            'PRECIOS SUPERIORES AL LOCAL',
            'PROPUESTA NO ACORDE CON PEDIDOSYA',
            'RECHAZO DE PEDIDOS',
            'REQUIERE VERIFICACIÓN COMERCIAL',
            'RESTAURANTE DEUDOR',
            'RESTAURANTE INCONTACTABLE',
            'SOLICITA BAJA POR COMISIÓN',
            'SOLICITA BAJA POR EXCLUSIVIDAD CON COMPETIDOR',
            'SOLICITA BAJA POR INCONVENIENTES CON ATENCIÓN AL CLIENTE',
            'PERFIL DUPLICADO',
            'SIN ESPECIFICAR',
            'PENDIENTE DE VENTAS',
            'PENDIENTE DE CONTENIDO',
            'PENDIENTE DE AUTOMATIZACIÓN')) then `r`.`disabled_reason`
            else ''
        end)
    end), '\r\n', ' '), ',', ' ')                           AS `disabled_reason`,
    replace(replace((
        case
        when (locate('|', `r`.`disabled_reason`) > 0) then substring_index(`r`.`disabled_reason`, '|', 1)
        else
        (case
            when (upper(`r`.`disabled_reason`) in ('CERRÓ SUS PUERTAS DEFINITIVAMENTE',
            'CERRÓ SUS PUERTAS TEMPORALMENTE',
            'CAMBIO DE DUEÑO',
            'DEJÓ DE HACER DELIVERY',
            'NO QUIERE AUTOMATIZARSE',
            'PRECIOS SUPERIORES AL LOCAL',
            'PROPUESTA NO ACORDE CON PEDIDOSYA',
            'RECHAZO DE PEDIDOS',
            'REQUIERE VERIFICACIÓN COMERCIAL',
            'RESTAURANTE DEUDOR',
            'RESTAURANTE INCONTACTABLE',
            'SOLICITA BAJA POR COMISIÓN',
            'SOLICITA BAJA POR EXCLUSIVIDAD CON COMPETIDOR',
            'SOLICITA BAJA POR INCONVENIENTES CON ATENCIÓN AL CLIENTE',
            'PERFIL DUPLICADO',
            'SIN ESPECIFICAR',
            'PENDIENTE DE VENTAS',
            'PENDIENTE DE CONTENIDO',
            'PENDIENTE DE AUTOMATIZACIÓN')) then `r`.`disabled_reason`
            else ''
        end)
    end), '\r\n', ' '), ',',' ')                            AS `disabled_motive`,
    (case
        when isnull(`r`.`billing_info_id`) then 0
        else `r`.`billing_info_id`
    end)                                                    AS `billing_info_id`,
    ifnull(`food_category`.`food_category_id`, 0)           AS `main_cousine_category_id`,
    `r`.`link`                                              AS `link`,
    `r`.`logo`                                              AS `logo`,
    (case
        when isnull(`r`.`reception_system_enabled`) then 0
        else `r`.`reception_system_enabled`
    end)                                                    AS `reception_system_enabled`,
    cast(`rbi`.`marketing_amount` as decimal(19, 2))        AS `publicity_cost`,
    `rbi`.`automation_amount`                               AS `automation_amount`,
    (ifnull(`dz`.`has_shipping_amount`, 0)
            or (ifnull(`r`.`shipping_amount`, 0) > 0))      AS `has_shipping_amount`,
    `r`.`orders_reception_system_id`                        AS `orders_reception_system_id`,
    `r`.`orders_secondary_reception_system_id`              AS `orders_secondary_reception_system_id`,
    `rbi`.`logistics_commission`                            AS `logistics_commission`,
    `r`.`delivery_type`                                     AS `delivery_type`,
    `r`.`business_type`                                     AS `business_type`,
    ifnull(`r`.`accepts_vouchers`, 0)                       AS `accepts_vouchers`,
    (case
        when ((`r`.`state` in (1, 4))
                and (`r`.`type` = 3)) then 1
        else 0
    end)                                                    AS `is_online`,
    (case
        when ((`r`.`state` not in (1, 4))
                or (`r`.`type` <> 3)) then 1
        else 0
    end)                                                    AS `is_offline`,
    (case
        when (cast(`r`.`registered_date` as date) = cast((now() + interval -(1) day) as date)) then 1
        else 0
    end)                                                    AS `is_new_registered`,
    `r`.`shipping_amount`                                   AS `shipping_amount`,
    `r`.`shipping_amount_is_percentage`                     AS `shipping_amount_is_percentage`,
    `r`.`salesforce_id`                                     AS `salesforce_id`,
    `r`.`branch_parent_id`                                  AS `branch_parent_id`,
    `r`.`affected_by_porygon_events`                        AS `affected_by_porygon_events`,
    `r`.`affected_by_porygon_optimizations`                 AS `affected_by_porygon_optimizations`,
    `r`.`public_phone`                                      AS `public_phone`,
    `r`.`automatic_phone`                                   AS `automatic_phone`,
    `r`.`last_updated`                                      AS `last_updated`,
    `r`.`parallel_reception_system`                         AS `parallel_reception_system`,
    `r`.`menu_id`                                           AS `menu_id`,
    (case
        when ((`r`.`accepts_vouchers` = 1)
                and (`rs`.`supports_vouchers` = 1)
                and (isnull(`rs2`.`supports_vouchers`)
                        or (`rs2`.`supports_vouchers` = 1))) then 1
        else 0
    end)                                                    AS `accepts_and_supports_vouchers`,
    (case
        when ((`r`.`accepts_vouchers` = 0)
                and (`rs`.`supports_vouchers` = 1)
                and (isnull(`rs2`.`supports_vouchers`)
                        or (`rs2`.`supports_vouchers` = 1)
                    )
            ) then 1
        else 0
    end)                                                    AS `declines_but_supports_vouchers`,
    `r`.`kitchen_id`                                        AS `kitchen_id`,
    `r`.`concept_id`                                        AS `concept_id`,
    coalesce(`r`.`business_category_id`, 0)                 AS `business_category_id`,
    `r`.`accepts_pre_order`                                 AS `accepts_pre_order`,
    coalesce(`r`.`requires_proof_of_delivery`, FALSE)       AS `requires_proof_of_delivery`,
    `r`.`is_darkstore`                                      AS `is_darkstore`,
    `r`.`identity_card_behaviour`                           AS `identity_card_behaviour`,
    coalesce(`bi`.`single_commission`, FALSE)               AS `single_commission`,
    `r`.`shopper_type`                                      AS `shopper_type`
from
    ((((((((((((((`peyadb`.`restaurant` `r`
        left join `peyadb`.`address` `a` on ((`r`.`address_id` = `a`.`id`)))
        left join `peyadb`.`billing_info` `bi` on ((`bi`.`id` = `r`.`billing_info_id`)))
        left join `peyadb`.`reception_system` `rs` on ((`rs`.`id` = `r`.`orders_reception_system_id`)))
        left join `peyadb`.`reception_system` `rs2` on ((`rs2`.`id` = `r`.`orders_secondary_reception_system_id`)))
        left join `peyadb`.`restaurant_billing_info` `rbi` on ((`rbi`.`restaurant_id` = `r`.`id`)))
        left join `peyadb`.`area` `ar` on ((`ar`.`id` = `a`.`area_id`)))
        ------------------------------------
        -- ULTIMO CONTACTO
        ------------------------------------
        left join (
            select
                `con`.`restaurant_id` AS `restaurant_id`,
                max(`con`.`id`) AS `con_id`
            from `peyadb`.`contact` `con`
            where ((`con`.`type` = '1') and (`con`.`is_deleted` = 0))
            group by `con`.`restaurant_id`) `con_max` on ((`con_max`.`restaurant_id` = `r`.`id`)))
        left join `peyadb`.`contact` `con` on (((`con`.`restaurant_id` = `r`.`id`) and (`con`.`id` = `con_max`.`con_id`))))
        ------------------------------------
        -- CANTIDAD DE ONLINE Payment Methods
        ------------------------------------
        left join (
            select
                `s`.`restaurant_payment_methods_id` AS `restaurant_payment_methods_id`,
                count(1) AS `online_payments`
            from (`peyadb`.`restaurant_payment_method` `s`
                        join `peyadb`.`payment_method` `pm` on ((`pm`.`id` = `s`.`payment_method_id`)))
            where ((`pm`.`online` = 1)
                        and (`pm`.`is_deleted` is false))
            group by `s`.`restaurant_payment_methods_id`) `online` on ((`online`.`restaurant_payment_methods_id` = `r`.`id`)))
        left join (
            select
                `dz`.`restaurant_id` AS `restaurant_id`,
                ifnull(min((case when (`dz`.`min_delivery_amount` = 0) then `r`.`min_delivery_amount` else `dz`.`min_delivery_amount` end)),
                0) AS `min_delivery_amount`,
                ifnull(max((case when (`dz`.`min_delivery_amount` = 0) then `r`.`min_delivery_amount` else `dz`.`min_delivery_amount` end)),
                0) AS `max_delivery_amount`,
                ifnull(min((case when (`dz`.`shipping_amount` = 0) then `r`.`shipping_amount` else `dz`.`shipping_amount` end)),
                0) AS `min_shipping_amount`,
                ifnull(max((case when (`dz`.`shipping_amount` = 0) then `r`.`shipping_amount` else `dz`.`shipping_amount` end)),
                0) AS `max_shipping_amount`,
                (case
                    when ((max(`dz`.`min_delivery_amount`) > 0)
                    or (`r`.`min_delivery_amount` > 0)) then 1
                    else 0
                end) AS `has_MOV`,
                count(distinct `dz`.`id`) AS `cantidad_zonas`,
                (max(ifnull(`dz`.`shipping_amount`, 0)) > 0) AS `has_shipping_amount`
            from
                (`peyadb`.`delivery_zone` `dz`
            join `peyadb`.`restaurant` `r` on
                ((`r`.`id` = `dz`.`restaurant_id`)))
            where
                (`dz`.`is_deleted` = 0)
            group by
                `dz`.`restaurant_id`) `dz` on ((`dz`.`restaurant_id` = `r`.`id`)))
        left join `peyadb`.`delivery_time` `dt` on ((`dt`.`id` = `r`.`delivery_time_id`)))
        left join (
            select
                `rr`.`restaurant_id` AS `restaurant_id`,
                sum(`rr`.`valid_review_count`) AS `cant_com`
            from
                `peyadb`.`restaurant_rating` `rr`
            where
                (`rr`.`platform` = 1)
            group by
                `rr`.`restaurant_id`) `comm` on ((`comm`.`restaurant_id` = `r`.`id`)))
        left join `peyadb`.`menu` on ((`peyadb`.`menu`.`id` = `r`.`menu_id`)))
        left join (
            select
                `c`.`restaurant_id` AS `restaurant_id`,
                `c`.`food_category_id` AS `food_category_id`,
                `f`.`name` AS `main_cousine`
            from
                ((((
                select
                    `c`.`restaurant_id` AS `restaurant_id`,
                    max(`c`.`id`) AS `id`
                from
                    (`peyadb`.`restaurant_food_category` `c`
                join (
                    select
                        `c`.`restaurant_id` AS `restaurant_id`,
                        min(`c`.`sorting_index`) AS `sorting_index`,
                        max(`c`.`percentage`) AS `percentage`
                    from
                        `peyadb`.`restaurant_food_category` `c`
                    where
                        ((`c`.`enabled` = 1)
                        and (`c`.`state` in (1, 3, 5)))
                    group by
                        `c`.`restaurant_id`) `x` on
                    (((`x`.`restaurant_id` = `c`.`restaurant_id`)
                    and (`x`.`sorting_index` = `c`.`sorting_index`)
                    and (`x`.`percentage` = `c`.`percentage`))))
                group by
                    `c`.`restaurant_id`)) `y`
            join `peyadb`.`restaurant_food_category` `c` on (((`c`.`restaurant_id` = `y`.`restaurant_id`)
                                                                and (`c`.`id` = `y`.`id`))))
            left join `peyadb`.`food_category` `f` on ((`c`.`food_category_id` = `f`.`id`))
        )
    ) `food_category` on ((`food_category`.`restaurant_id` = `r`.`id`)))