all: qcsvm_train_c qcsvm_train_r qcsvm_features_c qcsvm_features_r

qcsvm_features_c:
	cd cmd/qcsvm_features_c && go install

qcsvm_features_r:
	cd cmd/qcsvm_features_r && go install

qcsvm_train_c:
	cd cmd/qcsvm_train_c && go install

qcsvm_train_r:
	cd cmd/qcsvm_train_r && go install

.PHONY: qcsvm_train_c qcsvm_train_r qcsvm_features_c qcsvm_features_r
