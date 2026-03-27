Quick Start (run from ~/Desktop/amazon-ads-ai/)
                                                                                                                               
  # Activate the environment                                                                                                   
  source .venv/bin/activate                                                                                                    
                                                                                                                               
  # 1. Seed demo data (already done - skip if re-running)                                                                      
  python scripts/seed_demo_data.py
                                                                                                                               
  # 2. Compute features                                                                                                        
  python scripts/run_features.py                                                                                               
                                                                                                                               
  # 3. Train models
  python scripts/run_training.py                                                                                               
                  
  # 4. Run optimization (predictions → bids → budgets → keywords)                                                              
  python scripts/run_optimization.py
                                                                                                                               
  # 5. Start the API (http://localhost:8000)                                                                                   
  PYTHONPATH=. uvicorn src.api.main:app --port 8000                                                                            
                                                                                                                               
  # 6. Start the dashboard (http://localhost:8501)                                                                             
  PYTHONPATH=. streamlit run src/dashboard/app.py --server.port 8501     