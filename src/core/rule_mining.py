import os
import logging
from data_manager.data_processing import DataProcessor
from mlxtend.frequent_patterns import fpgrowth, association_rules
from data_manager.preprocessing.rule_mining_processor import RuleMiningProcessor

def get_rules(input_file, output_dir, time_column, time_format, sensors, date_range, core_processing_par, time_processing_par, rule_mining_processing_par):
    """
  This function handles the common steps required for a specified time range, or full data options. It processes the data, prepares the data, and then generates the rules.
  """
    # step 1: loads the portion of the data that we need, then process it
    data_processor = DataProcessor(input_file, output_dir, time_column, time_format, sensors, core_processing_par, time_processing_par)

    if date_range:
        start_date, end_date = date_range
        _, _, processed_data = data_processor.process_time_range(start_date, end_date)
    else:
        _, _, processed_data = data_processor.process_full_data()

    # step 2: preprocess for the rule mining tool
    method, bins, labels, continuous_sensor_types, min_support, min_confidence, min_lift = rule_mining_processing_par

    discretize_data = RuleMiningProcessor(processed_data, sensors, time_column)
    discretize_data = discretize_data.advanced_preprocessing(method, bins, labels, continuous_sensor_types)

    mining_rules_file = os.path.join(output_dir, "processed_data_mining_rules.csv")
    discretize_data.to_csv(mining_rules_file, index=False)

    # step 3: run association rule mining
    rules = run_association_rule_mining(discretize_data, min_support, min_confidence, min_lift)

    # step 4: format and display rules
    formatted_rules = format_rules_output(rules)
    
    # step 5: write the rules to a text file
    mining_rules_file = os.path.join(output_dir, "generated_rules.txt")
    with open(mining_rules_file, "w") as file:
        file.write(formatted_rules)
    logging.info(f"Formatted rules generated and saved in {mining_rules_file}")

    return formatted_rules

def run_association_rule_mining(discretized_data, min_support, min_confidence, min_lift):
    """
  This function runs the FP-Growth algorithm and extracts association rules from the discretized data.
  """
    logging.info(f"Running the FP-Growth algorithm and the extraction of the association rules started.")

    # step 1: run FP-Growth to find frequent itemsets
    frequent_itemsets = fpgrowth(discretized_data, min_support=min_support, use_colnames=True)

    # check if frequent itemsets were found
    if frequent_itemsets.empty:
        logging.warning("No frequent itemsets were found. Consider lowering min_support.")
        return frequent_itemsets  # Return empty result to avoid errors

    logging.info(f"Number of frequent itemsets found: {len(frequent_itemsets)}")

    # step 2: generate association rules
    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)

    # check if rules were generated
    if rules.empty:
        logging.warning("No association rules were generated. Consider lowering min_confidence.")
        return rules  # Return empty result to avoid errors

    logging.info(f"Number of association rules generated: {len(rules)}")

    # step 3: filter rules by confidence, support, and lift
    rules = rules[(rules['confidence'] > 0) & (rules['support'] > 0)]
    
    if min_lift:
        rules = rules[rules["lift"] >= min_lift]

    return rules

def format_rules_output(rules):
    """
  This function formats the association rules into a more user-friendly output.
  """
    if rules.empty:
        return "No valid association rules were generated."

    formatted_output = []
    for index, row in rules.iterrows():
        rule_str = (f"Rule {index + 1}: If {list(row['antecedents'])} "
                    f"then {list(row['consequents'])} "
                    f"(Support: {row['support']:.3f}, Confidence: {row['confidence']:.3f}, Lift: {row['lift']:.3f})")
        formatted_output.append(rule_str)

    return '\n'.join(formatted_output)
