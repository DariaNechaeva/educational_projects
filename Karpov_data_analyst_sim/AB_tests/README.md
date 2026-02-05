# AB test

This project is divided into two phases: ensuring the reliability of the experimental infrastructure through A/A test
and evaluating a new post-recommendation algorithm via A/B test. The analysis focuses on CTR as the primary metric.  

### Part 1: A/A Test Validation  

Before analyzing the experiment, the splitting system was validated to ensure no inherent bias existed between user groups.  

*Methodology:* Repeated subsampling (bootstrapping approach) from the control population.  

*Procedure:* Conducted multiple iterations of t-tests on random sub-samples to calculate
the percentage of cases where the null hypothesis was rejected.  

*Goal:* Verify that the False Positive Rate (Type I error) 
matches the significance level (α=0.05), confirming the splitting system functions correctly.  

### Part 2: A/B Test Analysis (Post Recommendation Algorithm)

*Objective:* Determine if the new algorithm significantly increases the CTR.

*Statistical Methods Applied*

To ensure a comprehensive view, the groups were compared using several techniques:

- T-test: Assessing differences in mean CTR.  

- Mann-Whitney U Test: Non-parametric evaluation of the distributions.  

- Poisson Bootstrap: To estimate the distribution of the metric.  

- Smoothed CTR (α=5): Reducing noise in user-level metrics.  

- Bucket Transformation: Aggregating data into "buckets" to normalize distributions for T-test and Mann-Whitney applications.

#### Tech Stack
Language: Python  
Libraries: Pandas, NumPy, SciPy, Matplotlib, Seaborn, Hashlib    
Database: ClickHouse 
