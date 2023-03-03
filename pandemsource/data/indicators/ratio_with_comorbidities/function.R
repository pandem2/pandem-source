#patients_with_comorbidities / number_of_patients
0.22 * ifelse(bed_type == "icu", 3, 2)
