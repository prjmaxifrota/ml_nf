class RowClassifier:
    def __init__(self):
        # Dictionary with condition lambdas, weight scores, and codes for descriptions
        self.stat_condition_lookups = [
            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] < 0.1 and row['count'] > 50) and 
                                (abs(row['avg'] - row['median']) < 0.05 * row['avg']),
            'weight_score': 5, 'description_code': 'cons_good_sym'},

            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] < 0.1 and row['count'] > 50),
            'weight_score': 4.5, 'description_code': 'cons_good'},

            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] >= 0.1) and 
                                (row['stddev'] / row['avg'] > 0.5),
            'weight_score': 4.5, 'description_code': 'pot_risk_high_var'},

            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] >= 0.1) and 
                                (row['max'] - row['min'] > 0.5 * row['avg']),
            'weight_score': 4.5, 'description_code': 'pot_risk_high_vol'},

            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] < 0.1 and row['count'] > 50) and 
                                (row['sum'] > row['avg'] * row['count']),
            'weight_score': -4, 'description_code': 'cons_bad_high_accum'},

            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] < 0.1 and row['count'] > 50),
            'weight_score': -3.5, 'description_code': 'cons_bad'},

            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] >= 0.1) and 
                                (row['max'] > 1.5 * row['avg']),
            'weight_score': -5, 'description_code': 'vol_high_peaks'},

            {'lambda': lambda row: (row['count'] > 5) and 
                                (row['max'] / (row['min'] + 1e-6) > 5),
            'weight_score': -4, 'description_code': 'range_disp_high'},

            {'lambda': lambda row: (row['count'] > 5 and row['stddev'] >= 0.1),
            'weight_score': -4, 'description_code': 'high_impact_vol'},

            {'lambda': lambda row: (row['count'] < 5),
            'weight_score': 1, 'description_code': 'stat_insig'}
        ]
        
        # Dictionary for descriptions based on language
        self.stat_description_dict = {
            'pt-BR': {
                'cons_good_sym': 'Bom Consistente com Simetria',
                'cons_good': 'Bom Consistente',
                'pot_risk_high_var': 'Risco Potencial com Alta Variabilidade',
                'pot_risk_high_vol': 'Risco Potencial com Alta Volatilidade',
                'generally_good': 'Geralmente Bom',
                'cons_bad_high_accum': 'Ruim Consistente com Alto Acúmulo',
                'cons_bad': 'Ruim Consistente',
                'vol_high_peaks': 'Volátil com Grandes Picos',
                'range_disp_high': 'Disparidade de Alto Alcance',
                'high_impact_vol': 'Alto Impacto Volátil',
                'pot_risk': 'Risco Potencial',
                'stat_insig': 'Sem relevância estatística'
            },
            'en-US': {
                'cons_good_sym': 'Consistently Good with Symmetry',
                'cons_good': 'Consistently Good',
                'pot_risk_high_var': 'Potential Risk with High Variability',
                'pot_risk_high_vol': 'Potential Risk with High Volatility',
                'generally_good': 'Generally Good',
                'cons_bad_high_accum': 'Consistently Bad with High Accumulation',
                'cons_bad': 'Consistently Bad',
                'vol_high_peaks': 'Volatile with High Peaks',
                'range_disp_high': 'High Range Disparity',
                'high_impact_vol': 'High Impact Volatility',
                'pot_risk': 'Potential Risk',
                'stat_insig': 'Statistical Insignificance'
            }
        }
        
        self.ml_condition_lookups = [
            # Strong confidence with high reliability
            {'lambda': lambda row: row['consensus_count'] >= 2 and 
                                    (row['trend_detected'] == 'Consistent Trend' or row['model_agreement'] >= 2) and
                                    row['relationship'] == 'Clear Relationship' and
                                    row['performance_reliability'] >= 0.7,
            'ml_weight_score': 5, 'description_code': 'strong_consistent_rel_high_reliability', 'ml_good_or_bad': 'good'},

            # Moderate confidence with higher performance reliability
            {'lambda': lambda row: 2 <= row['consensus_count'] < 2 and 
                                    (row['trend_detected'] == 'Consistent Trend' or row['model_agreement'] >= 2) and
                                    row['relationship'] == 'Clear Relationship' and
                                    row['performance_reliability'] >= 0.5,
            'ml_weight_score': 4, 'description_code': 'moderate_consistent_rel_good_performance', 'ml_good_or_bad': 'good'},

            # Lower confidence, but reliable performance and consistent
            {'lambda': lambda row: row['consensus_count'] <= 1 and 
                                    (row['trend_detected'] == 'Consistent Trend' or row['model_agreement'] >= 1) and
                                    row['relationship'] == 'Clear Relationship' and
                                    row['performance_reliability'] >= 0.4,
            'ml_weight_score': 3, 'description_code': 'weak_consistent_rel_reliable', 'ml_good_or_bad': 'good'},

            # Potential anomaly with inconsistent patterns
            {'lambda': lambda row: row['relationship'] == 'Potential Anomaly' or
                                    (row['trend_detected'] == 'Inconsistent Trend' or row['model_agreement'] < 1),
            'ml_weight_score': -5, 'description_code': 'potential_anomaly_inconsistent_patterns', 'ml_good_or_bad': 'bad'},

            # Inconsistent trends but clear relationships, with moderate model agreement
            {'lambda': lambda row: row['trend_detected'] == 'Inconsistent Trend' and 
                                    row['relationship'] == 'Clear Relationship' and 
                                    row['model_agreement'] >= 2,
            'ml_weight_score': -3, 'description_code': 'inconsistent_trends_clear_rel', 'ml_good_or_bad': 'bad'},

            # Conflicting patterns with low agreement and low reliability
            {'lambda': lambda row: row['consensus_count'] < 1 and 
                                    (row['trend_detected'] == 'Inconsistent Trend' or row['relationship'] == 'Potential Anomaly') and
                                    row['model_agreement'] < 1 and
                                    row['performance_reliability'] < 0.4,
            'ml_weight_score': -4, 'description_code': 'conflicting_patterns_low_agreement', 'ml_good_or_bad': 'bad'},

            # Default unclassified category for cases that don’t match other conditions
            {'lambda': lambda row: True,  
            'ml_weight_score': 0, 'description_code': 'unclassified', 'ml_good_or_bad': 'unclassified'}
        ]


        # Dictionary for ML descriptions by language code
        self.ml_description_dict = {
            'pt-BR': {
                'strong_consistent_rel': 'Alta confiança com padrão consistente e claro',
                'moderate_consistent_rel': 'Confiança moderada com padrão consistente e claro',
                'weak_consistent_rel': 'Baixa confiança, mas padrão consistente e claro',
                'potential_anomaly': 'Anomalia potencial com relações pouco claras',
                'inconsistent_trends': 'Tendências inconsistentes, mas com relações claras',
                'conflicting_patterns': 'Padrões conflitantes, tendências inconsistentes com anomalias potenciais',
                'unclassified': 'Não classificado',
                
                # New Descriptions
                'strong_consistent_rel_high_reliability': 'Alta confiança com confiabilidade elevada e padrão consistente',
                'moderate_consistent_rel_good_performance': 'Confiança moderada e desempenho satisfatório com padrão consistente',
                'weak_consistent_rel_reliable': 'Baixa confiança, mas desempenho confiável e padrão consistente',
                'potential_anomaly_inconsistent_patterns': 'Anomalia potencial com padrões inconsistentes',
                'inconsistent_trends_clear_rel': 'Tendências inconsistentes, mas com relações claras e confiáveis',
                'conflicting_patterns_low_agreement': 'Padrões conflitantes, baixo consenso e tendências inconsistentes'
            },
            'en-US': {
                'strong_consistent_rel': 'High confidence with a consistent, clear pattern',
                'moderate_consistent_rel': 'Moderate confidence with a consistent, clear pattern',
                'weak_consistent_rel': 'Low confidence but consistent, clear pattern',
                'potential_anomaly': 'Potential anomaly with unclear relationships',
                'inconsistent_trends': 'Inconsistent trends but clear relationships',
                'conflicting_patterns': 'Conflicting, inconsistent trends with potential anomalies',
                'unclassified': 'Unclassified',
                
                # New Descriptions
                'strong_consistent_rel_high_reliability': 'High confidence with high reliability and consistent pattern',
                'moderate_consistent_rel_good_performance': 'Moderate confidence and satisfactory performance with consistent pattern',
                'weak_consistent_rel_reliable': 'Low confidence but reliable performance and consistent pattern',
                'potential_anomaly_inconsistent_patterns': 'Potential anomaly with inconsistent patterns',
                'inconsistent_trends_clear_rel': 'Inconsistent trends but clear, reliable relationships',
                'conflicting_patterns_low_agreement': 'Conflicting patterns with low agreement and inconsistent trends'
            }
        }

    def classify_statistical_row(self, row):
        """
        Classify a row based on predefined conditions and return the associated weight score and description code.
        """
        for condition in self.stat_condition_lookups:
            if condition['lambda'](row):
                description_code = condition['description_code']
                weight_score = condition['weight_score']
                description = self.get_description(description_code, lang='pt-BR', type='stat')
                return weight_score, description, description_code   
        
        return 0, 'Sem classificação estatística', 'no_match'  # Default if no conditions match

    def classify_ml_row(self, row):
        """
        Classify a row based on ML conditions and return the associated weight score and description code.
        """
        for condition in self.ml_condition_lookups:
            if condition['lambda'](row):
                return { 
                    'description_code': condition['description_code'],
                    'ml_weight_score': condition['ml_weight_score'],
                    'ml_good_or_bad': condition['ml_good_or_bad']
                }
        
         # Default if no conditions match
        return {
            'description_code': 'unclassified',
            'ml_weight_score': 0,
            'ml_good_or_bad': 'unclassified'
        }

    def get_description(self, description_code, lang='en-US', type='stat'):
        """
        Look up the description based on the provided code and language.
        Type can be 'stat' for statistical or 'ml' for ML descriptions.
        """
        if type == 'stat':
            return self.stat_description_dict.get(lang, {}).get(description_code, 'Description not found')
        elif type == 'ml':
            return self.ml_description_dict.get(lang, {}).get(description_code, 'Description not found')

    
    def get_combined_score(self, stat_weight, ml_weight, statistical_multiplier, ml_multiplier): 
        total_score = (stat_weight * statistical_multiplier) + (ml_weight * ml_multiplier)
        return total_score
    