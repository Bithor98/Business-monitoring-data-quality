# Business Monitoring & Data Quality Pipeline

Sistema de monitorización de negocio construido en Python que simula un **pipeline real de empresa** para:
- evaluar la calidad de los datos,
- depurarlos aplicando reglas de negocio,
- detectar anomalías relevantes,
- y generar alertas accionables para distintos equipos.

El objetivo no es visualizar métricas, sino **detectar problemas antes de que impacten en el negocio**.

---

## 🧠 Contexto del problema

En muchas empresas:
- los datos llegan con errores,
- los KPIs no son fiables,
- los problemas se detectan tarde,
- y el análisis se hace sobre datos sin validar.

Este proyecto aborda ese problema construyendo un **sistema automático de control y alerta**, similar a los que se usan en entornos reales de negocio.

---

## 🏗️ Arquitectura del pipeline
```
Raw Data
↓
Data Quality Checks
↓
Data Cleaning
↓
Re-validation
↓
Business Anomaly Detection
↓
Executive Summary
```
---

## 📂 Estructura del proyecto

```
project6-monitoring/
├─ data/
│ ├─ raw/ # Datos originales (con errores)
│ └─ processed/ # Datos limpios
├─ reports/ # Reportes generados
├─ src/
│ ├─ generate_data.py # Generación de datos realistas + errores
│ ├─ validate.py # Checks de calidad + quality score
│ ├─ clean.py # Limpieza con reglas de negocio
│ ├─ alerts.py # Detección de anomalías de negocio
│ └─ summary.py # Resumen ejecutivo
├─ main.py # Orquestación completa del pipeline
└─ README.md
```

---

## ✅ Funcionalidades principales

### 1. Data Quality Validation
- detección de duplicados
- control de nulos críticos
- ingresos negativos
- fechas fuera de rango
- cálculo de **Quality Score (0–100)**

### 2. Data Cleaning
- eliminación de duplicados
- eliminación de registros inválidos
- corrección de errores contables
- trazabilidad del impacto del clean

### 3. Business Monitoring
- detección de caídas bruscas de revenue
- detección de picos sospechosos
- tasas altas de cancelación
- clasificación por severidad

### 4. Executive Summary
- top alertas más relevantes
- reporte legible para perfiles no técnicos

---

## 🚨 Ejemplo de alertas detectadas

- Revenue cayó un **53%** respecto al día anterior  
- Pico anómalo de ingresos (z-score alto)  
- Tasa de cancelación diaria superior al umbral definido  

Cada alerta incluye:
- fecha
- tipo
- severidad
- métrica afectada
- explicación en lenguaje de negocio

---

## ▶️ Ejecución del pipeline completo

```bash
python main.py