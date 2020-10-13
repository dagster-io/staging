{{ config(materialized='table') }}
select
  name,
  mfr,
  type,
  calories / weight as normalized_calories,
  protein / weight as normalized_protein,
  fat / weight as normalized_fat,
  sodium / weight as normalized_sodium,
  fiber / weight as normalized_fiber,
  carbo / weight as normalized_carbo,
  sugars / weight as normalized_sugar,
  potass / weight as normalized_potass,
  vitamins / weight as normalized_vitamins,
  shelf,
  1 as weight,
  cups / weight as cups,
  rating
from "dbt-example-schema".cereals
