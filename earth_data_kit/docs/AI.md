# AI in Earth Data Kit

Earth Data Kit provides powerful AI capabilities for fine-tuning geospatial foundational models. The AI module is built on top of terra-torch and torchgeo, providing a robust framework for training and deploying geospatial machine learning models.

## Goal

The primary goal of the AI module is to enable efficient fine-tuning of geospatial models while:

1. Leveraging pre-trained foundational models
2. Supporting various geospatial data formats and sources
3. Providing flexible training configurations
4. Optimizing performance for large geospatial datasets
5. Enabling easy model deployment and inference
6. Facilitating efficient data annotation workflows

## Key Features

The AI module offers several key capabilities:

1. **Model Fine-tuning**: Fine-tune pre-trained models on geospatial data
2. **Data Processing**: Handle various geospatial data formats and sources
3. **Data Annotation**: Tools for efficient labeling and annotation of geospatial data
4. **Training Pipeline**: Flexible training configurations and optimizations
5. **Model Evaluation**: Comprehensive evaluation metrics for geospatial tasks
6. **Model Deployment**: Easy deployment and inference capabilities

## Example Use Case

A common use case is fine-tuning a pre-trained model on satellite imagery for land cover classification:

1. Initialize the model using name of the model. Also assign
2. Configure training parameters
3. Prepare and preprocess the training data
4. Annotate training data using interactive labeling tools
5. Fine-tune the model
6. Evaluate model performance
7. Deploy the model for inference

## API

1. `edk.ai.Model(name, pretrained=True)` - Creates a new model instance
2. `set_training_config(config)` - Sets training configuration parameters
3. `prepare_data(data_source, labels)` - Prepares and preprocesses training data
4. `annotate_data(data_source, annotation_type)` - Launches interactive annotation interface
5. `train(epochs, batch_size)` - Fine-tunes the model
6. `evaluate(test_data)` - Evaluates model performance
7. `predict(input_data)` - Makes predictions on new data
8. `save_model(path)` - Saves the trained model
9. `load_model(path)` - Loads a saved model

## Examples
