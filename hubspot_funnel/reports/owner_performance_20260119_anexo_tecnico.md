# Anexo Técnico: Owner Performance Analysis
**Generado:** 2026-01-19
**Referencia:** owner_performance_20260119.md

---

## 1. Conciliación de Ingresos (Inlasa S.A.S)

### Verificación del Deal

| Campo | Valor |
|-------|-------|
| Deal ID | 45092448392 |
| Nombre | Inlasa S.A.S |
| Monto | **$1,140,000** |
| Stage | Suscripción Activa (Pago) ✅ |
| Fecha Cierre | 2025-10-17 |
| Owner | Juan Pablo |
| Pipeline | LaHaus AI |
| Fuente | Offline |

### Hallazgo

El deal **está legitimamente ganado** con un monto de **$1,140,000 USD**. La discrepancia en el reporte principal se debe al corte temporal:

- **Periodo del reporte:** 2025-10-21 → 2026-01-19 (90 días)
- **Fecha de cierre Inlasa:** 2025-10-17 (4 días antes del corte)

El deal aparece en "Top Deals (Won)" porque esa sección usa 180 días de histórico, pero no se incluye en "Revenue Won" del período actual porque cerró antes del corte de 90 días.

### Corrección Recomendada

| Métrica | Valor Reportado | Valor Real (180d) |
|---------|-----------------|-------------------|
| Juan Pablo Revenue Won | $5,611 | **$1,145,611** |
| Ranking Revenue | #2 | **#1** |

**Conclusión:** No es un error de carga. El corte temporal de 90 días excluye este mega-deal. Considerar ampliar el periodo a 180 días para capturar deals de alto valor.

---

## 2. Segmentación por Fuente de Lead

### 2.1 Win Rate y No-Show por Canal y Owner

| Owner | Fuente | Total | Won | Lost | Win Rate | No-Show % |
|-------|--------|-------|-----|------|----------|-----------|
| **Juan Pablo** | Offline | 54 | 12 | 33 | **27%** | **18%** |
| Juan Pablo | Direct Traffic | 29 | 0 | 25 | 0% | 12% |
| Juan Pablo | Unknown | 13 | 3 | 7 | 30% | 0% |
| Juan Pablo | Other Campaigns | 12 | 1 | 10 | 9% | 0% |
| **Geraldine** | Offline | 48 | 11 | 32 | 26% | **47%** |
| Geraldine | Direct Traffic | 35 | 2 | 26 | 7% | 35% |
| Geraldine | Other Campaigns | 15 | 1 | 14 | 7% | 50% |
| **Yajaira** | Offline | 70 | 5 | 53 | 9% | **55%** |
| Yajaira | Direct Traffic | 42 | 3 | 31 | 9% | 32% |
| Yajaira | Other Campaigns | 15 | 0 | 13 | 0% | 15% |
| **Litzia** | Offline | 45 | 4 | 34 | 11% | 15% |
| Litzia | Direct Traffic | 38 | 3 | 29 | 9% | 41% |
| Litzia | Other Campaigns | 15 | 0 | 12 | 0% | 17% |

### 2.2 Resumen por Fuente (Todos los Owners)

| Fuente | Total | Won | Lost | Win Rate | No-Show % |
|--------|-------|-----|------|----------|-----------|
| **Offline** | 217 | 32 | 152 | **17%** | 36% |
| Direct Traffic | 144 | 8 | 111 | 7% | 31% |
| Other Campaigns | 57 | 2 | 49 | 4% | 22% |
| Unknown | 21 | 6 | 10 | 38% | 0% |
| Paid Social | 3 | 0 | 1 | 0% | 0% |

### Conclusión

**El problema de no-shows es de PROCESO, no de canal.**

Comparativa en el mismo canal (Offline):
- Juan Pablo: 18% no-show
- Geraldine: 47% no-show (+161%)
- Yajaira: 55% no-show (+206%)

Los tres reciben leads de la misma fuente "Offline" pero tienen tasas de no-show radicalmente diferentes. Esto indica que:

1. **No es culpa del canal** - el mismo canal produce resultados muy diferentes por owner
2. **Es un problema de proceso de confirmación** - Juan Pablo tiene un mejor sistema de seguimiento pre-demo
3. **Recomendación:** Documentar el proceso de confirmación de Juan Pablo y replicarlo

---

## 3. Auditoría de Higiene de Actividad

### 3.1 Verificación de Permisos API

| Endpoint | Status | Diagnóstico |
|----------|--------|-------------|
| Emails API | 403 Forbidden | ❌ Token sin scope `sales-email-read` |
| Calls API | 200 OK | ✅ Funcionando |
| Meetings API | 200 OK | ✅ Funcionando |

### 3.2 Actividades en CRM (últimos 90 días)

| Tipo | Total Registros |
|------|-----------------|
| Calls | 106 |
| Meetings | 627 |
| Emails | 0 (permiso denegado) |

### 3.3 Llamadas por Owner

| Owner | Llamadas | Status |
|-------|----------|--------|
| Geraldine | 52 | COMPLETED: 49, NO_ANSWER: 3 |
| Yajaira | 54 | COMPLETED: 54 |
| Juan Pablo | 0 | - |
| Litzia | 0 | - |

### Conclusión

| Issue | Causa Raíz | Impacto | Acción |
|-------|------------|---------|--------|
| 0 emails para todos | **Error de integración API** - token sin permisos de lectura de emails | No se pueden medir actividades de email | Solicitar scope `sales-email-read` en HubSpot |
| 0 llamadas Juan Pablo | **Falta de registro manual** - no usa el objeto "Calls" | Métricas de actividad subestimadas | Capacitar en registro de llamadas |
| 0 llamadas Litzia | **Falta de registro manual** - no usa el objeto "Calls" | Métricas de actividad subestimadas | Capacitar en registro de llamadas |

**Nota:** El API token necesita el scope `sales-email-read` para acceder al historial de emails. Los 0 emails NO significa que no envían emails, significa que el sistema no puede leerlos.

---

## 4. Análisis de Pérdida (Lost Reasons)

### 4.1 Frecuencia de Motivos de Pérdida

| Motivo | Count | % Total | Revenue Lost | Acumulado |
|--------|-------|---------|--------------|-----------|
| **No asistió al demo** | 162 | 27% | $465 | 27% |
| **Precio alto** | 157 | 26% | $16,550 | 53% |
| **Mal timing/Budget** | 91 | 15% | $20,300 | 69% |
| **No interesado** | 77 | 13% | $2,225 | **81%** ← Pareto |
| Higiene de lead | 53 | 9% | $12,474 | 90% |
| Riesgo/Complejidad | 29 | 5% | $10,277 | 95% |
| Eligió competidor | 11 | 2% | $3,093 | 97% |
| Falta funcionalidad | 7 | 1% | $0 | 98% |
| Solución in-house | 6 | 1% | $500 | 99% |
| Valor/ROI no claro | 3 | 1% | $0 | 100% |

**Total perdidos:** 596 deals | **Revenue perdido:** $65,884

### 4.2 Top 3 Motivos por Owner

| Owner | #1 | #2 | #3 |
|-------|----|----|----|
| Juan Pablo | Precio alto (40) | Mal timing/Budget (30) | No interesado (23) |
| Geraldine | No asistió al demo (40) | Higiene de lead (14) | No interesado (13) |
| Yajaira | No asistió al demo (41) | Mal timing/Budget (24) | Higiene de lead (17) |
| Litzia | Precio alto (98) | No asistió al demo (60) | No interesado (32) |

### 4.3 Análisis: ¿Por qué 71-82% de demos no convierten?

**Diagnóstico por Owner:**

| Owner | Perfil de Pérdida | Hipótesis |
|-------|-------------------|-----------|
| **Juan Pablo** | Precio alto (54%) | Leads llegan a demo pero no están calificados por presupuesto |
| **Geraldine** | No-show + Higiene (72%) | Problema de confirmación y calidad de leads |
| **Yajaira** | No-show + Timing (59%) | Saturación operativa + leads no calificados por timing |
| **Litzia** | Precio alto (53%) | Similar a Juan Pablo - falta calificación de presupuesto |

**Conclusiones:**

1. **Top 4 razones = 81%** de todas las pérdidas (Pareto confirmado)
2. **"Eligió competidor" es solo 2%** - la competencia NO es el problema principal
3. **Problema #1: No-shows (27%)** - leads que nunca llegan a ver el producto
4. **Problema #2: Precio (26%)** - falta calificación de presupuesto pre-demo
5. **Problema #3: Timing (15%)** - leads contactados en mal momento

### Inteligencia Competitiva

| Competidor | Menciones |
|------------|-----------|
| HeyGia | 1 |
| No especificado | 10 |

**Nota:** Solo 11 deals perdidos por competencia (2%). La competencia NO es un factor significativo de pérdida.

---

## 5. Análisis de Carga de Trabajo (Bandwidth)

### 5.1 Comparativa de Volumen por Periodo

| Owner | Prev 90d | Curr 90d | Δ Volume | Prev Win% | Curr Win% | Δ Win Rate |
|-------|----------|----------|----------|-----------|-----------|------------|
| Juan Pablo | 76 | 108 | +42% | 5% | 18% | **+12pp** |
| Geraldine | 79 | 103 | +30% | 15% | 17% | +1pp |
| **Yajaira** | **23** | **135** | **+487%** | **35%** | **9%** | **-26pp** |
| Litzia | 143 | 99 | -31% | 4% | 9% | +5pp |

### 5.2 Carga Diaria Promedio

| Owner | Prev (deals/día) | Curr (deals/día) | Δ Carga |
|-------|------------------|------------------|---------|
| Juan Pablo | 0.8 | 1.2 | +42% |
| Geraldine | 0.9 | 1.1 | +30% |
| **Yajaira** | **0.3** | **1.5** | **+487%** |
| Litzia | 1.6 | 1.1 | -31% |

### 5.3 Velocidad de Cierre: Yajaira

| Periodo | Avg Días Cierre | N deals |
|---------|-----------------|---------|
| Prev 90d | 47.0 | 23 |
| Curr 90d | 24.6 | 110 |
| **Delta** | **-22.4 días** | |

### Conclusión: Evidencia de Saturación Operativa

**Hallazgo paradójico:** Yajaira está cerrando MÁS RÁPIDO pero PERDIENDO MÁS.

| Indicador | Antes | Ahora | Diagnóstico |
|-----------|-------|-------|-------------|
| Volumen | 0.3 deals/día | 1.5 deals/día | **5x más carga** |
| Win Rate | 35% | 9% | **Caída de 26pp** |
| Días a cierre | 47 días | 24.6 días | **Más rápido pero superficial** |
| No-show rate | (bajo) | 41% | **Confirmaciones descuidadas** |

**Hipótesis de saturación confirmada:**
1. Yajaira tenía excelente performance con baja carga (35% win rate)
2. Se le asignaron 5x más deals de golpe
3. Para manejar el volumen, está procesando más rápido (24 vs 47 días)
4. La velocidad viene a costa de:
   - Menor seguimiento pre-demo (41% no-show)
   - Menos tiempo de nurturing
   - Cierre apresurado sin calificación

**Recomendación:** Redistribuir carga. Yajaira NO puede manejar 1.5 deals/día con calidad. Su capacidad óptima parece ser ~0.5 deals/día basado en su performance histórica.

---

## Resumen Ejecutivo de Hallazgos

| # | Hallazgo | Severidad | Acción Recomendada |
|---|----------|-----------|---------------------|
| 1 | Inlasa $1.14M es legítimo pero fuera del corte | Info | Usar periodo de 180d para reportes de revenue |
| 2 | No-shows son problema de PROCESO, no de canal | 🔴 Crítico | Replicar proceso de Juan Pablo (18% vs 47-55%) |
| 3 | API token sin permisos de emails | 🟡 Medio | Solicitar scope `sales-email-read` |
| 4 | Juan Pablo y Litzia no registran llamadas | 🟡 Medio | Capacitación en registro de actividades |
| 5 | 81% de pérdidas = No-show + Precio + Timing + No interesado | 🔴 Crítico | Implementar BANT qualification pre-demo |
| 6 | Competencia solo 2% de pérdidas | Info | NO es prioridad - enfocarse en proceso |
| 7 | Yajaira saturada: +487% volumen = -26pp win rate | 🔴 Crítico | Redistribuir carga a máx 0.5 deals/día |

---

*Anexo generado automáticamente por owner_performance_analysis.py*
