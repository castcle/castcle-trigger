# Castcle's Data Science Description
## 1. Workflow process
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

## 2. Collections description
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

## 3. Repositiory Description
There are 2 repositories those involved with data science process i.e. [castcle-trigger](https://github.com/castcle/castcle-trigger) and [castcle-ds-predict](https://github.com/castcle/castcle-ds-predict) which have similar structure but different functionality. More precisely, one is response for part 1. and 2., while another is response for part 3. of workflow process, respectively. 

In this section we will describe only collections those are interacted as output from data science processes which are located in this repository. For another data science-related collections [click here](https://github.com/castcle/castcle-ds-predict).
 1. [requirements.txt](https://github.com/castcle/castcle-trigger/blob/develop/requirements.txt): contains necessary libraries.
 2. [serverless.yml](https://github.com/castcle/castcle-trigger/blob/develop/serverless.yml): contains configuration.
 3. python caller files (.py): responses for calling main function in [modules](https://github.com/castcle/castcle-trigger/tree/develop/modules),
 - [castcle-trigger](https://github.com/castcle/castcle-trigger)
  - [x] [content_stats_update.py](https://github.com/castcle/castcle-trigger/blob/develop/content_stats_update.py): responses for calling to execute    [update_content_stats.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/update_content_stats/update_content_stats.py) to update `analytics-db.contentStats`.
  - [x] [creator_stats_update.py](https://github.com/castcle/castcle-trigger/blob/develop/creator_stats_update.py): responses for calling to execute    [update_creator_stats.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/update_creator_stats/update_creator_stats.py) to update `analytics-db.creatorStats`.
  - [x] [coldstart_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/coldstart_trainer.py): responses for calling to execute [coldstart_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/coldstart_prediction/coldstart_trainer.py) to update `analytics-db.mlArtifacts_country`.
  - [x] [personalize_content_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/personalize_content_trainer.py): responses for calling to execute [personalize_content_trainer.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/personalized_content/personalize_content_trainer.py) to update `analytics-db.mlArtifacts`.
  - [x] [topic_classification.py](https://github.com/castcle/castcle-trigger/blob/develop/topic_classification.py): responses for calling to execute [topic_classification.py](https://github.com/castcle/castcle-trigger/blob/develop/modules/topic_classify/topic_classification.py) to update `analytics-db.topics` and `app-db.contentinfo`.

## 4. Model Explanation: Cold-Start
This model will be used to rank within threshold contents based on countries' engagement behaviors. The model will be re-trained everyday in the morning and stored in mlArtifact_country collection in db_analytics. The model is for users that still do not have their own personalized model and can be used to give a wider range of content recommendation.
  1. Model inputs
  - Country engagement 
  - Content features 
  - Country code ( 250 countries, iso3166)

  2. Model detail
  - Model Used : XGBOOST Regression Model
  - Target variable : Weight engagement 
  - Time Using : 1 mins (12/13/2021)
  - Countries that do not have training data will adopt 'us' model

  3. Output
  - Collection contains countryCode, artifacts, time-stamp

  4. Model workflow (Training Section)
  This file explain only model training section. If you would like to see another section, [click here](https://github.com/castcle/castcle-ds-predict/edit/develop-docds/README.md)
   4.1. Prep engagement data ( app db engagement )
     1. Engagement List
     - Like
     - Comment 
     - Quote
     - Recast
     2. Aggregation : Sum
     3. Group By : countryCode (iso3166), contentId
     
   4.2. Prep content features ( analytics db contentStats, creatorStats )
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
    
  4.3 Weight key metrics and create target value ( like, comment, recast, quote )
  
  4.4 Learn from enrich dataset and save ML artifacts ( analytics db mlArtifacts_country)
   Output List
    - account (countryCode)
    - model
    - artifact
    - features
    - trainedAt

![Cold-start](https://user-images.githubusercontent.com/90676485/146301272-4d2cbb07-5810-48b1-ac91-0fddeb04905c.jpg)
