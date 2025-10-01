import torch
import torch.nn as nn

class DummyModel(nn.Module):
    def __init__(self):
        super(DummyModel, self).__init__()
        self.linear = nn.Linear(1, 1)
    
    def forward(self, x):
        return self.linear(x)

# Create and save TorchScript model
model = DummyModel()
model.eval()

# Create dummy input for tracing
dummy_input = torch.randn(1, 1)

# Convert to TorchScript
traced_model = torch.jit.trace(model, dummy_input)

# Save the model
traced_model.save('model.pth')
print("Created valid TorchScript model.pth")