USE Test;

WITH CTE AS (
	SELECT plant_id AS plant,
			produced_material AS fin_material_id,
			produced_material_release_type AS fin_material_release_type,
			produced_material_production_type AS fin_material_production_type,
			produced_material_quantity AS fin_production_quantity,
			produced_material AS prod_material_id,
			produced_material_release_type AS prod_material_release_type,
			produced_material_production_type AS prod_material_production_type,
			produced_material_quantity AS prod_material_production_quantity,
			component_material AS component_id,
			component_material_release_type,
			component_material_production_type,
			component_material_quantity AS component_consumption_quantity,
			year,
			month
	FROM Materials
	WHERE [produced_material_release_type] = 'FIN'

	UNION ALL

	SELECT CTE.plant,
		cte.fin_material_id,
        cte.fin_material_release_type,
        cte.fin_material_production_type,
        cte.fin_production_quantity,

		m.produced_material AS prod_material_id,
		m.produced_material_release_type,
		m.produced_material_production_type,
		m.produced_material_quantity,

		m.component_material,
		m.component_material_release_type,
		m.component_material_production_type,
		m.component_material_quantity,
		m.year,
		m.month
	FROM Materials AS m
	JOIN CTE
		ON CTE.component_id = m.produced_material
		AND m.year = CTE.year
		AND m.month = CTE.month
		AND m.plant_id = CTE.plant
		AND m.produced_material_release_type = 'PROD'
)

SELECT 
		*
FROM CTE
WHERE prod_material_release_type != 'FIN'