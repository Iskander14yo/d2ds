import joblib
import numpy as np
import pymongo.errors
import pandas as pd
from pymongo import MongoClient
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_curve, roc_auc_score
import matplotlib.pyplot as plt
import seaborn as sns
import shap
from sklearn.metrics import confusion_matrix
from sklearn.calibration import calibration_curve, CalibratedClassifierCV
from catboost import CatBoostClassifier, Pool

from util_arrays import columns_for_drop, columns_with_leaver_status, columns_true_false_convert, catboost_array


def create_mongo_connection():
    try:
        # Менять соединение с монгой
        client = MongoClient("localhost", 27017)
        db = client.dota_db
        return client, db
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)


#mongo_client, mongo_db = create_mongo_connection()
#dataframe_match_collection = mongo_db.dataframe_match_collection


def get_data_from_db(collection):
    data = list(collection.find({}))
    bd_data = pd.DataFrame(data)
    return bd_data


def get_data_from_csv(filename):
    data = pd.read_csv(filename)
    return data


leaver_status_map = {'NONE': 0, 'DISCONNECTED': 1, 'AFK': 2, 'DISCONNECTED_TOO_LONG': 3, 'ABANDONED': 4}


def dataframe_preprocessing(dataframe, predict_items=False):
    if not predict_items:
        dataframe = dataframe.drop(columns_for_drop, axis=1)
        dataframe['didRadiantWin'] = dataframe['didRadiantWin'].astype(int)
    dataframe[columns_true_false_convert] = dataframe[columns_true_false_convert].astype(int)
    dataframe = dataframe.fillna(9999)
    dataframe[columns_with_leaver_status] = (dataframe[columns_with_leaver_status] != 'NONE').astype(int)
    print("Данные были успешно прочитаны из бд")
    return dataframe


def create_train_test_split(dataframe):
    X_train, X_test, y_train, y_test = train_test_split(dataframe.drop('didRadiantWin', axis=1),
                                                        dataframe['didRadiantWin'], test_size=0.25, random_state=42)
    print("Данные были успешно разделены на трэйн и тест")
    return X_train, X_test, y_train, y_test


def fit_random_forest(Xtrain, ytrain):
    best_rfc = RandomForestClassifier(n_estimators=300, min_samples_split=39, min_samples_leaf=18,
                                      max_features='sqrt', max_depth=20, bootstrap=True, random_state=42)
    best_rfc.fit(Xtrain, ytrain)
    print("Модель была успешно обучена")
    joblib.dump(best_rfc, 'random_forest.h5')
    print("Модель была успешно сохранена")
    return best_rfc


def predict_dataframe(new_df, model_name, preprocessed=True):
    best_model = joblib.load(model_name)
    print("Модель была успешно загружена")
    if preprocessed:
        pred = best_model.predict_proba(new_df)
    else:
        converted_df = dataframe_preprocessing(new_df)
        pred = best_model.predict_proba(converted_df)
    return pred


def predict_new_object(input_object, model_name):
    best_model = joblib.load(model_name)
    print("Модель была успешно загружена")
    object_df = pd.DataFrame(input_object)
    converted_df = dataframe_preprocessing(object_df)
    return best_model.predict_proba(converted_df)


def predict_csv_file(file_name, model_name, is_catboost=False):
    best_model = joblib.load(model_name)
    if not is_catboost:
        model_features = best_model.feature_names_in_
    else:
        model_features = best_model.feature_names_
    print("Модель была успешно загружена")
    pred_df = pd.read_csv(file_name)
    pred_df = pred_df[model_features]
    converted_df = dataframe_preprocessing(pred_df, True)
    pred = best_model.predict_proba(converted_df)[:, 1]
    print(pred)
    return pred


def analyse_metrics(pred_objects, y_true, model_name):
    best_model = joblib.load(model_name)
    prediction = best_model.predict(pred_objects)
    y_pred_prob = best_model.predict_proba(pred_objects)[:, 1]

    print("Accuracy score: ", accuracy_score(y_true, prediction))
    print("F1 score: ", f1_score(y_true, prediction))

    fpr, tpr, thresholds = roc_curve(y_true, y_pred_prob, pos_label=1)
    roc_auc = roc_auc_score(y_true, y_pred_prob)

    plot_roc_auc(fpr, tpr, roc_auc)


def calibrate_predict_proba(xtrain, ytrain, xtest, ytest, model_name, model_name_out):
    best_model = joblib.load(model_name)
    rdf_sig = CalibratedClassifierCV(best_model, cv=5)
    rdf_sig.fit(xtrain, ytrain)
    sig_pred_proba = rdf_sig.predict_proba(xtest)
    joblib.dump(rdf_sig, model_name_out)
    plt.figure(figsize=(7, 7))

    sig_true_proba, sig_proba = calibration_curve(ytest, sig_pred_proba[:, 1], n_bins=15)

    plt.plot(sig_proba, sig_true_proba, label='Sigmoid RandomForest', color='blue')
    plt.plot([0, 1], [0, 1], label='Perfect', linestyle='--', color='green')

    plt.xlabel('Mean predicted probability')
    plt.ylabel('Fraction of positives')
    plt.title('Calibration curves')
    plt.legend()
    plt.show()

def compare_minute_probabilities(x_test, model_name):
    samples = x_test.sample(10)
    best_model = joblib.load(model_name)
    pred = best_model.predict_proba(samples)[:, 1]
    samples = samples.reset_index()
    for index, row in samples.iterrows():
        print("-------------------------------Sample ", index+1, '----------------------------------------------')
        print("Current minute: ", row['currentMinute'], "\nCurrent gold lead: ", row
        ['minuteRadiantNetworthLead'], '\nCurrent win probability: ', pred[index])
        print("-------------------------------------------------------------------------------------------------")

def plot_roc_auc(fpr, tpr, roc_auc):
    plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
    # roc curve for tpr = fpr
    plt.plot([0, 1], [0, 1], 'k--', label='Random classifier')
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curve')
    plt.legend(loc="lower right")
    plt.show()


def plot_confusion_matrix(ypred, ytest):
    cm = confusion_matrix(ytest, ypred)
    ax = sns.heatmap(cm, annot=True, cmap='Blues')

    ax.set_title('Dota 2 matches confusion matrix\n\n')
    ax.set_xlabel('\nPredicted match results')
    ax.set_ylabel('Actual match results ')

    ## Ticket labels - List must be in alphabetical order
    ax.xaxis.set_ticklabels(['False', 'True'])
    ax.yaxis.set_ticklabels(['False', 'True'])

    ## Display the visualization of the Confusion Matrix.
    plt.show()


def fit_catboost_model(xtrain, ytrain, xtest, ytest):
    fit_data = Pool(data=xtrain, label=ytrain, cat_features=catboost_array)
    # Создание и обучение модели с автоматической обработкой категориальных данных
    cat_model = CatBoostClassifier(objective='CrossEntropy', learning_rate=0.15000000000000002, depth=4,
                               min_child_samples=8, iterations=50, boosting_type='Ordered', bootstrap_type='MVS')
    cat_model.fit(fit_data, eval_set=(xtest, ytest))
    joblib.dump(cat_model, 'catboost_model.h5')

def fit_full_catboost_model(xtrain, ytrain):
    fit_data = Pool(data=xtrain, label=ytrain, cat_features=catboost_array)
    # Создание и обучение модели с автоматической обработкой категориальных данных
    cat_model = CatBoostClassifier(objective='CrossEntropy', learning_rate=0.004, depth=5,
                               min_child_samples=24, iterations=500)
    cat_model.fit(fit_data)
    joblib.dump(cat_model, 'full_catboost_model.h5')


def plot_feature_importance(Xtrain, model_name):
    best_model = joblib.load(model_name)
    rf_d = dict(zip(Xtrain.columns, best_model.feature_importances_))
    rf_weights = pd.Series(rf_d)
    sns.barplot(y=rf_weights.index, x=rf_weights.values)
    plt.show()


#TODO я не успел запустить
def plot_shap(Xtrain, model_name):
    best_model = joblib.load(model_name)
    shap_explain = shap.Explainer(best_model)
    rand_samples = Xtrain.sample(n=1000)
    shap_val = shap_explain.shap_values(rand_samples)

    shap.summary_plot(shap_val, rand_samples, plot_type='dot', show=False)
    plt.show()


random_forest_name = 'random_forest.h5'
calibrated_random_forest_name = 'random_forest.h5'
catboost_name = 'catboost_model.h5'
calibrated_catboost_name = 'calibrated_catboost.h5'
full_catboost = 'full_catboost_model.h5'
if __name__ == '__main__':
    #df = get_data_from_db()
    df = get_data_from_csv("input_data.csv")
    preprocessed_df = dataframe_preprocessing(df)
    X_train, X_test, y_train, y_test = create_train_test_split(preprocessed_df)
    #fit_catboost_model(X_train, y_train, X_test, y_test)
    #fit_random_forest(X_train, y_train)
    fit_full_catboost_model(xtrain=preprocessed_df.drop('didRadiantWin', axis=1),
                            ytrain=preprocessed_df['didRadiantWin'])
    analyse_metrics(X_test, y_test, full_catboost)
    compare_minute_probabilities(X_test, full_catboost)
    #predict_csv_file('for_prediction.csv', catboost_name, is_catboost=True)
    #plot_feature_importance(X_train, calibrated_catboost_name)
    pred = np.rint(predict_dataframe(X_test, full_catboost)[:,1])
    plot_shap(X_train, full_catboost)
    plot_confusion_matrix(pred, y_test)
    #calibrate_predict_proba(X_train, y_train, X_test, y_test, catboost_name, 'calibrated_catboost.h5')

