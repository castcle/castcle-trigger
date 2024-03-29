# Castcle's Data Science Description
## 1. Feed Algorithm Overview
Cascle's feed algorithm has tailored from 2 regimes i.e. **App** and **Analytics** which is undergo by some parts of data science processes. The database coloring represents location such as blue color stands for `app-db`, while green color represents `analytics-db`. This file will explain only **Analytics** regime with mild touch the **App**. Features are extracted in **Feature Extractors** i.e. [content_stats_update](https://github.com/castcle/castcle-trigger/blob/main/content_stats_update.py) and [creator_stats_update](https://github.com/castcle/castcle-trigger/blob/main/creator_stats_update.py) to facilitating **Model Training** and **Model Prediction** of such [coldstart_trainer](https://github.com/castcle/castcle-trigger/blob/main/coldstart_trainer.py), [personalize_content_trainer](https://github.com/castcle/castcle-trigger/blob/main/personalize_content_trainer.py) and [coldstart_predictor](https://github.com/castcle/castcle-ds-predict/blob/main/coldstart_predictor.py), [per_ct_predictor](https://github.com/castcle/castcle-ds-predict/blob/main/per_ct_predictor.py), respectively.

Moreover, **Aggregators** are respond to logically filter contents from **Inventory** in different ways. **Ranker** consumes features from **Feature Extractors** , Trained model artifact from **Model Training** and aggregated/filtered contents from **Aggregators** to ranking/scoring tend of engagement for the user using **Model Prediction** then feeds a certain amount to user's UI. More precisely, user interaction or engagement will be recorded to database for purpose of further **Analytics** and model evaluation.

![castcle_ds_overviewdrawio drawio (7)](https://user-images.githubusercontent.com/90676485/147076633-6a638b28-27d2-40d3-9afb-b12987f639e7.png)

## 2. Workflow Process
Data science workflow process of Castcle can be illustrated by the bottom diagram of the below figure exhibits overall workflow process interacts across databases i.e. blue blocks represent collections in `app-db` and green blocks represents collections in `analytics-db` databases in Mongodb Atlas, respectively. The bold arrows stand for presence of entity key relation between collections, the dot arrows reflect data extraction by either aggregation or calculation, and the two-headed arrows represents swap event.

In the other hands, the workflow process can be separated in to 3 steps which are shown as the top orange blocks in below diagram (orange arrows) corresponding in vertical axis to the bottom diagram. However, non-machine learning (ML)-involved processes will only carry 2 steps (purple arrow).

Moreover, this file describes steps 1., 2. and a part of step 3., another part of step 3. is described in [castcle-ds-predict](https://github.com/castcle/castcle-ds-predict).

![castcle_ds_er drawio (5)](https://user-images.githubusercontent.com/90676485/145951393-3af8140e-cc63-429e-b034-94b63de75dfb.png)
 
### 1. feature preparation
Schedule of updating features involved with 3 processes using **Mongodb aggregation framework** and are set to execute every hour for ML model utilization. 

  - **update content stats** employs Mongodb Atlas aggregation framework to extract data of such amount of engagement, amount of attachments, amount of character of message and age score using decay function of the recent updated contents from `app-db.contents` then upsert to `analytics-db.contentStats`
  - **update creator stats** also extracts data of such amount of engagement, amount of content, age score both overall and summarized by content type of the recent updated users  from `app-db.contents` including amount of followers, amount of following, geolocation from `app-db.users` and `app-db.accounts` then upsert to `analytics-db.creatorStats`

![castcle_ds_er_feature_prepare drawio (2)](https://user-images.githubusercontent.com/90676485/145942254-41b10706-89fb-416f-bf08-707f79be55cb.png)

  - **topics classify** employs various APIs and is deployed in Mongodb realm database trigger which is executed whenever `app-db.contents` has created or updated. There are 2 outputs from a single execution, one is language which is classified in 3 conditions orderly i.e. consider to be Thai language, non-Thai known languages, and unknown language, respectively. Another is topic classification of English content, conditionally executes when the language of content is English and contain more than token threshold value after special character cleaning.
  
![castcle_ds_er_topics drawio](https://user-images.githubusercontent.com/90676485/145948071-b17b16a0-ea6e-42f3-a62c-75e4603f9848.png)

### 2. **Modeling**
Execute as schedule to prepare trained model artifact for further prediction usage. There are 2 main processes in this step which are coldstart for guest users and personalize content for registered users.
  - **coldstart trainer** construct model artifact for guest users using XGboost libraries which will be discuss in the section 4., there 250 artifacts those are aligned by country code as follow ISO3166 using features from both `analytics-db.contentStats` and `analytics-db.creatorStats` and considers engagement from `app-db.engagements`. The country-based model artifacts are upserted into `analytics-db.mlArtifacts_country`.
  - **personalize content trainer** contructs personal model artifact for every registered user by taking features from both `analytics-db.contentStats` and `analytics-db.creatorStats` and consider `app-db.engagements` then schedule upserted the artifacts to `analytics-db.mlArtifacts` for further prediction propose. 

![castcle_ds_er_model drawio (1)](https://user-images.githubusercontent.com/90676485/145948315-36ccde10-674d-4943-bde7-58e57ac88ca3.png)

### 3. Saving Persistent
This step possesses response for both schedule and on demand execution available for guest and register users, respectively. There are also 2 processes in this step which are coldstart for guest users and personalize content for registered users which are located in [castcle-ds-predict](https://github.com/castcle/castcle-ds-predict). Another function to saving persistent is **topic classify** which is described below,
 - **topic classify** This executes together with **topics classify** in feauture preparation step but function in saving persistent. Both outputs from feature preparation step will be then upserted to `app-db.contentinfo` and hierachicaly upserted to `analytics-db.topics`, respectively.

![castcle_ds_er_topic_persist drawio](https://user-images.githubusercontent.com/90676485/145951328-be7f1a1b-d023-4217-8be2-4e1ab721cab3.png)

## 3. Collections description
In this section we will describe only collections those are interacted as output from data science processes. The data science-related collections can be separated into 2 groups depend on its location.
  1. collections in `analytics-db`
  - `contentStats`: prepares feature for model utilization in part of content and is obtained by extracting data from `app-db.contents`. It updates itself every hour by removing contents which have age over threshold and upsert outputs from **update content stats**.
  - `creatorStats`: prepares feature for model utilization in part of content creator user and is obtained by extracting data from `app-db.contents`. Similar to `contentStats`, it updates itself every hour but keep all content creator user using outputs from **update creator stats**.
  - `mlArtifacts`: collects personalize content model artifacts which are the output from **personalize content trainer** and also collect model version.
  - `mlArtifacts_country`: collects country-based model artifacts which are the output from **coldstart trainer** and also collect model version.
  - `topics`: master collection of topics that is gathered hiearachically; contain children and parents topics (if have) from outputs of **topics classify**. It updates when `app-db.contents` has created or updated and topics can be classified.  
 
  2. collections in `app-db`
  - `contentinfo`: duplicated collection amount with `app-db.contents` and updates when `app-db.contents` has created or updated in the same process from **topics classify**. It collects both language and topics of the contents.
  - `guestfeeditemstemp`: It is output from **coldstart predictor** designed for eliminate downtime which will swap with `guestfeeditems` after successfully when upserted and contains item type.
  - `guestfeeditems`: a collection that exists for utilize as feed to guest users which is swap from `guestfeeditemstemp`. It is output from **coldstart predictor** and also contains item type.

## 4. Repositiory Description
There are 2 repositories those involved with data science process i.e. [castcle-trigger](https://github.com/castcle/castcle-trigger) and [castcle-ds-predict](https://github.com/castcle/castcle-ds-predict) which have similar structure but different functionality. More precisely, one is response for part 1. and 2., while another is response for part 3. of workflow process, respectively. 

In this section we will describe only collections those are interacted as output from data science processes which are located in this repository. For another data science-related collections [click here](https://github.com/castcle/castcle-ds-predict).
 1. [requirements.txt](https://github.com/castcle/castcle-trigger/blob/develop/requirements.txt): contains necessary libraries.
 2. [serverless.yml](https://github.com/castcle/castcle-trigger/blob/develop/serverless.yml): contains configuration.
 3. python caller files (.py): responses for calling main function in [modules](https://github.com/castcle/castcle-trigger/tree/develop/modules),
 - [x] [content_stats_update.py](https://github.com/castcle/castcle-trigger/blob/develop/content_stats_update.py): responses for calling to execute    [update_content_stats.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/update_content_stats/update_content_stats.py) to update `analytics-db.contentStats`.
 - [x] [creator_stats_update.py](https://github.com/castcle/castcle-trigger/blob/develop/creator_stats_update.py): responses for calling to execute    [update_creator_stats.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/update_creator_stats/update_creator_stats.py) to update `analytics-db.creatorStats`.
 - [x] [coldstart_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/coldstart_trainer.py): responses for calling to execute [coldstart_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/coldstart/coldstart_trainer.py) to update `analytics-db.mlArtifacts_country`.
 - [x] [personalize_content_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/personalize_content_trainer.py): responses for calling to execute [personalize_content_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/personalized_content/personalize_content_trainer.py) to update `analytics-db.mlArtifacts`.
 - [x] [topic_classification.py](https://github.com/castcle/castcle-trigger/blob/develop/topic_classification.py): responses for calling to execute [topic_classification.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/topic_classify/topic_classification.py) to update `analytics-db.topics` and `app-db.contentinfo`.

## 5. Model Explanation: Cold-Start
This model will be used to ranking/scoring within threshold contents based on engagement behavior in each specified country. The model will be re-trained everyday then stored in `analytics-db.mlArtifacts_country` collection. These models support users those do not have their own personalized model and can be used to give a wider range of content recommendation.
  1. Model inputs
  - Country engagement 
  - Content features 
  - Country code (250 countries, ISO3166)

  2. Model detail
  - Model: XGBOOST Regression Model
  - Target variable: Weight engagement 
  - Time using: 1 mins (12/13/2021)
  - Countries that do not have training data will adopt "us" model

  3. Output
  - Collection contains "countryCode", model artifacts, and timestamp

  4. Model workflow
  This file explain only model training section. If you would like to see another section, [click here](https://github.com/castcle/castcle-ds-predict/blob/develop/README.m)
  
   4.1. Engagement data preparation
     
     1. Engagement List
     - Like
     - Comment 
     - Quote
     - Recast
     
     2. Aggregation : Sum
     
     3. Group By : "countryCode" (ISO3166), "contentId" 
     
   4.2. Content features preparation
     
     1. Content Feature List
     - likeCount : Total like of each content based on subject
     - commentCount : Total comment of each content based on subject 	
     - recastCount : Total recast of each content based on subject 	
     - quoteCount : Total quote of each content based on subject 
     - photoCount : Total photo of each content	
     - characterLength : Number of charecter	
     - creatorContentCount : Total content of the creator of this content
     - creatorLikedCount : Total like of the creator of this content 
     - creatorCommentedCount : Total comment of the creator of this content 
     - creatorRecastedCount : Total recast of the creator of this content 
     - creatorQuotedCount : Total quote of the creator of this content
     - ageScore : age score of this content
     
     2. Aggregation : Sum, Count
     
     3. Group By : contentId
  
  4.3 Weight key metrics and create target value (like, comment, recast, quote)
  
  4.4 Learn from enrich dataset and save model artifacts
   Output List
    - "account" (as "countryCode")
    - "model"
    - "artifact"
    - "features"
    - "trainedAt"
    
![Cold-start (1)](https://user-images.githubusercontent.com/90676485/147532352-1af3e94d-93d4-43f5-af1c-643cb62c260a.jpg)

## 6. Model Explanation: Personalized Content Model
This model will be used to ranking/scoring the requested contents based on user’s engagement behaviors. The model will be re-trained everyday then stored in `analytics-db.mlArtifacts`. These models support users that have their own personalized model meaning that they have at least one engagement history and can be used to give a wider range of content recommendation combined with cold start model.
 
 1. Model inputs
 - User engagement 
 - Content features 

 2. Model detail 
 - Model Used: XGBOOST Regression Model
 - Target variable : Weight engagement 
 - Time using : 1 mins (12/13/2021)

 3. Output
 - Collection contains "userId", model artifacts, and timestamp 

 4. Model workflow
 
 4.1. Engagement data preparation
   
    1. Engagement List
    - Like
    - Comment 
    - Quote
    - Recast
  
  2. Aggregation : Sum
  
  4. Group By: "userId", "contentId"
  
 4.2. Content features preparation
  
    1. Content Feature List
    - likeCount: Total like of each content based on subject
    - commentCount: Total comment of each content based on subject 	
    - recastCount: Total recast of each content based on subject 	
    - quoteCount: Total quote of each content based on subject 
    - photoCount: Total photo of each content	
    - characterLength: Number of charecter	
    - creatorContentCount: Total content of the creator of this content
    - creatorLikedCount: Total like of the creator of this content 
    - creatorCommentedCount: Total comment of the creator of this content 
    - creatorRecastedCount: Total recast of the creator of this content 
    - creatorQuotedCount: Total quote of the creator of this content
    - ageScore: age score of this content
  
  2. Aggregation: Sum, Count
  
  3. Group By: "contentId"
 
 4.3.Weight key metrics and create target value ( like, comment, recast, quote )
 
 4.4.Learn from enrich dataset and save model artifacts
  
    1. Output List
    - "account"
    - "model"
    - "artifact"
    - "features"
    - "trainedAt"
    
![Personalized_content](https://user-images.githubusercontent.com/90676485/147532327-d78bc510-9953-4c35-99d8-8953db47cb76.jpg)

## 7. Fraud Detection

### Overview

A bot is a program used to produce automated tasks on a platform or service. Often, it is created for immoral purposes to make trouble for other people or generate fraudulent activities. Detecting and preventing bad bots becomes an essential capability to keep the quality of your platform or service.

To detect bot users in the Castle application, we planned to build a model that can learn user behaviors to gain the ability to be suspicious of the users whose behaviors seem to be a bot and send them to administrators for later verification.

For this feature, we separate the functionality into four parts: feature extraction, model training, prediction, and feature update.

![fraud_detection_workflow](https://user-images.githubusercontent.com/98333717/159420244-a64a45ef-6bda-4c84-9672-f0618ff7162d.png)

### Four parts of fraud detection functionality

**1. Feature extraction**
> For each 500-latest-post reading of a user, we extract all features using descriptive statistics to generate the absolute skewness, absolute kurtosis, and standard deviation (normalized by the mean) of durations of reading a post and durations between pairs of consecutive seen posts. The following are the features extracted from the data:
> - The absolute skewness of durations of reading a post (postReadingTimeAbsSkew)
> - The absolute kurtosis of durations of reading a post (postReadingTimeAbsKurt)
> - The normalized standard deviation of durations of reading a post (postReadingTimeNormStd)
> - The normalized standard deviation of durations between pairs of consecutive seen posts (postReadingTimeDifferenceNormStd)

**2. Model training**
> To train a model for this fraud detection task, we use the verified data (the extracted features with verification status from the application) as a dataset for the training process. The algorithm we used to learn the dataset is the one-class classification based on principal component analysis (PCA) by defining the bot cases as inliers. For the human cases, we define them as outliers because they have an inconsistent pattern in the feature space, making it hard to learn a class boundary. In addition, we evaluate the performance of the trained model with the following metrics: accuracy, precision, recall, and f1-score.

**3. Prediction**
> We use the trained model to detect users whose behaviors are suspected to be a bot and send them to the verification process of the application to determine their status as true (actual bot) or false (false bot).

**4. Feature update**
> After the verification process by the application, each suspicious user will have the verification status to indicate which one is an actual bot or not. These updates will be valuable feedback and become new complements of the existing dataset used in the model improvement process in the future by retraining the model with a larger and fresher dataset.

### Related data

**1. app-db.feeditems**
> - seenCredential
> - seenAt
> - offScreenAt

**2. analytics-db.credentialfeatures**
> - seenCredential
> - firstSeenAt
> - lastSeenAt
> - count
> - postReadingTimeAbsSkew
> - postReadingTimeAbsKurt
> - postReadingTimeNormStd
> - postReadingTimeDifferenceNormStd
> - verificationStatus
> - verifiedAt
> - createdAt
> - updatedAt

**3. app-db.suspiciouscredentials**
> - seenCredential
> - firstSeenAt
> - lastSeenAt
> - verificationStatus
> - verifiedAt
> - createdAt

**4. analytics-db.frauddetectionmlartifacts**
> - model
> - dataset
> - features
> - model_classes
> - artifact
> - evaluationReport
> - trainedAt
